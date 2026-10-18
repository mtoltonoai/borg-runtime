//! Consumer tests use a controlled controller-HTTP receipt seam. They do not
//! claim controller reservation/credit qualification or make a provider POST.
use super::*;
use crate::{
    client::ApiClient,
    native::{
        CategoryDiagnostic, DiagnosticBodyState, HttpDiagnostic, HttpStatusDiagnostic,
        NumericDiagnostic, ProviderErrorCategory, ProviderFailureClass, ProviderRejection,
        QuotaDiagnostic, QuotaDimension, QuotaValues, RetryAfter, RetryHeaderDiagnostic,
        SchedulerRetryDiagnostic,
    },
    protocol::{
        Attempt, AttemptPhase, Definition, ExecutionProfile, ExecutorKind, InferenceSettings,
        Limits, Task, TaskStatus,
    },
    worker::{WorkerConfig, process, test_control},
};
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{
    Request as HttpRequest, Response as HttpResponse, body::Incoming, service::service_fn,
};
use hyper_util::rt::TokioIo;
use std::{
    convert::Infallible,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64},
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::{oneshot, watch};

type Hold = (oneshot::Sender<()>, oneshot::Receiver<()>);
#[derive(Default)]
struct Script {
    requests: Vec<Request>,
    original: Option<Report>,
    reservation: Option<(Report, Task)>,
    reservation_query: Option<oneshot::Sender<()>>,
    lose_next_ack: bool,
    wrong_original: bool,
    refuse_append: bool,
    hold_append: Option<Hold>,
}
struct Fixture {
    reporter: Reporter,
    input: TaskInput,
    config: NativeConfig,
    script: Arc<Mutex<Script>>,
    server: tokio::task::JoinHandle<()>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}
impl Fixture {
    fn admission(&self, index: u32) -> Admission {
        preflight(&self.reporter, &self.input, &self.config, index).unwrap()
    }
    fn path(&self, report: &Report) -> PathBuf {
        self.reporter
            .outbox
            .join(format!("{}.json", component(&report.report_id)))
    }
    fn control(&self) -> (watch::Sender<bool>, Control) {
        let (tx, rx) = watch::channel(false);
        (tx, test_control(self.input.limits.deadline_unix_ms, rx))
    }
    fn sends(&self) -> usize {
        self.script
            .lock()
            .unwrap()
            .requests
            .iter()
            .filter(|r| matches!(r.command, Operation::ObserveWorker { .. }))
            .count()
    }
}

async fn fixture(label: &str) -> Fixture {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("artifacts/v2/worker-native-request-diagnostics/tests")
        .join(format!(
            "{label}-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
    std::fs::create_dir_all(root.parent().unwrap()).unwrap();
    std::fs::create_dir(&root).unwrap();
    let policy = Arc::new(RuntimeLimits::seed().unwrap());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let script = Arc::new(Mutex::new(Script::default()));
    let serving = script.clone();
    let server = tokio::spawn(async move {
        let mut connections = tokio::task::JoinSet::new();
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let script = serving.clone();
            connections.spawn(async move {
                let service = service_fn(move |request: HttpRequest<Incoming>| {
                    let script = script.clone();
                    async move {
                        let request: Request = serde_json::from_slice(
                            &request.into_body().collect().await.unwrap().to_bytes(),
                        )
                        .unwrap();
                        let (reply, lose, hold) = {
                            let mut state = script.lock().unwrap();
                            state.requests.push(request.clone());
                            match &request.command {
                                Operation::GetOperation { operation_id } => {
                                    let reply = if operation_id
                                        == &contract::reservation_id("diagnostic-attempt", 1)
                                    {
                                        if let Some(ready) = state.reservation_query.take() {
                                            let _ = ready.send(());
                                        }
                                        if let Some((original, task)) = &state.reservation {
                                            let mut record = lookup_record(original).unwrap();
                                            record.outcome = Some(
                                                serde_json::to_value(Outcome::Ok {
                                                    data: Data::Task(Box::new(task.clone())),
                                                })
                                                .unwrap(),
                                            );
                                            Reply {
                                                request_id: request.request_id.clone(),
                                                outcome: Outcome::Ok {
                                                    data: Data::Operation(record),
                                                },
                                                commit: Some(MutationCommit {
                                                    operation_id: operation_id.clone(),
                                                    controller_revision: 11,
                                                }),
                                                observed_controller_revision: Some(23),
                                            }
                                        } else {
                                            Reply::refused(
                                                &request.request_id,
                                                "not_found",
                                                "no original reservation",
                                            )
                                        }
                                    } else if let Some(original) = &state.original {
                                        assert_eq!(operation_id, &original.report_id);
                                        let mut original = original.clone();
                                        if state.wrong_original {
                                            original.generation += 1;
                                        }
                                        Reply {
                                            request_id: request.request_id.clone(),
                                            outcome: Outcome::Ok {
                                                data: Data::Operation(
                                                    lookup_record(&original).unwrap(),
                                                ),
                                            },
                                            commit: Some(MutationCommit {
                                                operation_id: operation_id.clone(),
                                                controller_revision: 17,
                                            }),
                                            observed_controller_revision: Some(23),
                                        }
                                    } else {
                                        Reply::refused(
                                            &request.request_id,
                                            "not_found",
                                            "no original operation",
                                        )
                                    };
                                    (reply, false, None)
                                }
                                Operation::ObserveWorker { report } => {
                                    if state.refuse_append {
                                        (
                                            Reply::refused(
                                                &request.request_id,
                                                "invalid_input",
                                                "controlled complete refusal",
                                            ),
                                            false,
                                            None,
                                        )
                                    } else {
                                        if let Some(original) = &state.original {
                                            assert_eq!(original, report);
                                        } else {
                                            state.original = Some(report.clone());
                                        }
                                        let reply = Reply {
                                            request_id: request.request_id.clone(),
                                            outcome: Outcome::Ok {
                                                data: Data::Acknowledged,
                                            },
                                            commit: Some(MutationCommit {
                                                operation_id: report.report_id.clone(),
                                                controller_revision: 17,
                                            }),
                                            observed_controller_revision: Some(17),
                                        };
                                        let lose = std::mem::take(&mut state.lose_next_ack);
                                        (reply, lose, state.hold_append.take())
                                    }
                                }
                                _ => panic!(
                                    "diagnostic delivery must use only original report and lookup"
                                ),
                            }
                        };
                        if let Some((entered, release)) = hold {
                            let _ = entered.send(());
                            let _ = release.await;
                        }
                        let response = if lose {
                            HttpResponse::builder()
                                .status(503)
                                .body(Full::new(Bytes::new()))
                                .unwrap()
                        } else {
                            HttpResponse::new(Full::new(Bytes::from(
                                serde_json::to_vec(&reply).unwrap(),
                            )))
                        };
                        Ok::<_, Infallible>(response)
                    }
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .await;
            });
        }
    });
    let config = NativeConfig {
        endpoint: "http://127.0.0.1:1/v1/responses".into(),
        api_key_env: "OPENAI_API_KEY".into(),
        model: crate::native::MODEL.into(),
    };
    let mut input = TaskInput { instruction_source: None,
        pull_request: None,
        resolved_limits: None,
        repository_read: None,
        instruction: "Controlled terminal diagnostic fixture".into(),
        definition: Definition {
            charter: None,
            limit_overrides: None,
            execution_profile: Some(ExecutionProfile::CollaborationV1 { web_search: None }),
            inference: Some(InferenceSettings::selected()),
            id: "diagnostic-agent".into(),
            revision: "diagnostic-agent-1".into(),
            executor: ExecutorKind::Native,
        },
        repository: None,
        acceptance_commands: vec![],
        output_schema: None,
        limits: Limits {
            deadline_unix_ms: process::now_ms() + 60_000,
            model_requests: 3,
            output_bytes: 4096,
            result_bytes: policy.task_defaults.result_bytes,
            max_delegation_depth: 1,
        },
        dependencies: vec![],
        native_workflow: None,
    };
    input.resolved_limits = Some(policy.resolve_task(&input).unwrap());
    let reporter = Reporter {
        resource: None,
        client: ApiClient::new(&url, policy.clone()).unwrap(),
        config: WorkerConfig {
            limits: policy.clone(),
            resource_profile: None,
            controller_url: url,
            id: "diagnostic-worker".into(),
            incarnation: "original-incarnation".into(),
            workspace_root: root.clone(),
            codex_path: "/usr/bin/false".into(),
            coding_capacity: 0,
            native_capacity: 1,
            native_endpoint: config.endpoint.clone(),
            validation_executable: std::env::current_exe().unwrap(),
        },
        attempt: Attempt {
            id: "diagnostic-attempt".into(),
            session_id: "diagnostic-session".into(),
            task_id: "diagnostic-task".into(),
            generation: 1,
            worker_id: "diagnostic-worker".into(),
            worker_incarnation: "original-incarnation".into(),
            phase: AttemptPhase::Running,
            deadline_unix_ms: input.limits.deadline_unix_ms,
            stopped: false,
            uncertain: false,
        },
        outbox: root,
        sequence: Arc::new(AtomicU64::new(1)),
        executor_seen: Arc::new(AtomicBool::new(false)),
        provider_uncertain: Arc::new(AtomicBool::new(false)),
        uncertainty_retained: Arc::new(AtomicBool::new(false)),
        output_limit: input.limits.output_bytes,
        result_limit: input.limits.result_bytes,
        coding_limits: policy.execution.coding.clone(),
        outbox_lock: Arc::new(tokio::sync::Mutex::new(())),
    };
    let original = Report {
        report_id: contract::reservation_id(&reporter.attempt.id, 1),
        worker_id: reporter.config.id.clone(),
        incarnation: reporter.config.incarnation.clone(),
        task_id: reporter.attempt.task_id.clone(),
        attempt_id: reporter.attempt.id.clone(),
        generation: reporter.attempt.generation,
        observation: Observation::Usage {
            model_requests: 1,
            input_tokens: None,
            output_tokens: None,
            native_diagnostic_reserved: true,
        },
    };
    let task = Task {
        id: reporter.attempt.task_id.clone(),
        session_id: reporter.attempt.session_id.clone(),
        input: input.clone(),
        assigned_agent_id: input.definition.id.clone(),
        status: TaskStatus::Active,
        revision: 3,
        attempt: Some(reporter.attempt.clone()),
        selected_checkpoint: None,
        result: None,
        blocked_reason: None,
        pending_assignment: None, native_retry_block: None,
        progress: String::new(),
        recent_output: String::new(),
        output_truncated: false,
        parent_task_id: None,
        waiting_child_id: None,
        completed_child: None,
    };
    script.lock().unwrap().reservation = Some((original, task));
    Fixture {
        reporter,
        input,
        config,
        script,
        server,
    }
}
fn binding() -> ProviderRequestBinding {
    // Controlled sent body b"{}"; this is not an inferred provider request.
    ProviderRequestBinding {
        bytes: 2,
        sha256: "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a".into(),
    }
}
fn unknown() -> ProviderFailureDiagnostic {
    ProviderFailureDiagnostic::new(ProviderFailureClass::TransportUnknown)
}
fn retained(f: &Fixture) -> RetainedDiagnostic {
    retain_with_credentials(
        &f.reporter,
        &f.admission(1),
        ProviderOutcome::Unknown,
        Some(&binding()),
        Some(&unknown()),
        &[],
    )
    .unwrap()
}
fn largest_facts(native: &NativeLimits) -> ProviderFailureDiagnostic {
    assert!(native.error_body_bytes >= 5);
    ProviderFailureDiagnostic::http(
        ProviderFailureClass::HttpResponsePending,
        HttpDiagnostic {
            status: HttpStatusDiagnostic::Value(999),
            body: DiagnosticBodyState::AdvertisedBound,
            category: CategoryDiagnostic::Value(ProviderErrorCategory::AuthenticationFailed),
            scheduler_retry_after: SchedulerRetryDiagnostic::Value(RetryAfter::DelayMs(u64::MAX)),
            retry_after: RetryHeaderDiagnostic::DateUnixMs(u64::MAX),
            retry_after_ms: NumericDiagnostic::Value(u64::MAX),
            requests_limit: NumericDiagnostic::Value(u64::MAX),
            requests_remaining: NumericDiagnostic::Value(u64::MAX),
            tokens_limit: NumericDiagnostic::Value(u64::MAX),
            tokens_remaining: NumericDiagnostic::Value(u64::MAX),
            requests_reset_ms: NumericDiagnostic::Value(u64::MAX),
            tokens_reset_ms: NumericDiagnostic::Value(u64::MAX),
            quota: QuotaDiagnostic::Value(QuotaValues {
                dimension: QuotaDimension::RequestsPerMinute,
                limit: format!("0.{}", "0".repeat(native.error_body_bytes - 4)),
                used: "0".into(),
                requested: "0".into(),
                wait_ms: u64::MAX,
            }),
        },
    )
}

#[tokio::test]
async fn complete_diagnostic_lookup_envelope_has_exact_boundary_and_no_dispatch() {
    let f = fixture("envelope").await;
    let admission = f.admission(1);
    let facts = largest_facts(&admission.native);
    facts.validate(&admission.native).unwrap();
    assert_eq!(
        serde_json::to_vec(&facts).unwrap().len(),
        ProviderFailureDiagnostic::maximum_encoded_bytes(&admission.native).unwrap()
    );
    let mut report = admission.original.clone();
    if let Observation::NativeRequestDiagnostic { failure, .. } = &mut report.observation {
        *failure = Some(facts);
    }
    let extra = ProviderFailureDiagnostic::maximum_encoded_bytes(&admission.native).unwrap() - 4;
    let outcome = Outcome::Ok {
        data: Data::Operation(lookup_record(&report).unwrap()),
    };
    let exact = serde_json::to_vec(&outcome).unwrap().len();
    let mut policy = admission.accepted.clone();
    policy.controller.outcome_bytes = exact;
    assert!(envelope(&admission.original, extra, &policy).is_ok());
    policy.controller.outcome_bytes = exact - 1;
    assert!(
        envelope(&admission.original, extra, &policy)
            .unwrap_err()
            .contains("original lookup")
    );
    assert!(f.script.lock().unwrap().requests.is_empty());
    assert_eq!(std::fs::read_dir(&f.reporter.outbox).unwrap().count(), 0);
}

#[tokio::test]
async fn preflight_checks_startup_scan_outbox_and_original_admission() {
    let mut f = fixture("admission").await;
    assert!(preflight(&f.reporter, &f.input, &f.config, 0).is_err());
    let mut policy = (*f.reporter.config.limits).clone();
    policy.worker.credential_scan_units = 1;
    policy.validate().unwrap();
    f.reporter.config.limits = Arc::new(policy);
    assert!(
        preflight(&f.reporter, &f.input, &f.config, 1)
            .err()
            .unwrap()
            .contains("credential_scan_units")
    );
    f.reporter.config.limits = Arc::new(RuntimeLimits::seed().unwrap());
    let mut policy = (*f.reporter.config.limits).clone();
    policy.worker.credential_scan_depth = 5;
    policy.validate().unwrap();
    f.reporter.config.limits = Arc::new(policy);
    assert!(
        preflight(&f.reporter, &f.input, &f.config, 1)
            .err()
            .unwrap()
            .contains("credential_scan_depth")
    );
    f.reporter.config.limits = Arc::new(RuntimeLimits::seed().unwrap());
    let mut policy = (*f.reporter.config.limits).clone();
    policy.worker.outbox_records = 1;
    policy.worker.outbox_batch = 1;
    policy.validate().unwrap();
    f.reporter.config.limits = Arc::new(policy);
    std::fs::write(f.reporter.outbox.join("held.json"), b"{}").unwrap();
    assert!(
        preflight(&f.reporter, &f.input, &f.config, 1)
            .err()
            .unwrap()
            .contains("outbox_records")
    );
    assert_eq!(
        std::fs::read(f.reporter.outbox.join("held.json")).unwrap(),
        b"{}"
    );
    assert!(f.script.lock().unwrap().requests.is_empty());
}

#[tokio::test]
async fn cancelled_not_started_is_durable_before_delivery_or_outbox_lock() {
    let f = fixture("already-cancelled").await;
    let admission = f.admission(1);
    let (cancel, control) = f.control();
    cancel.send(true).unwrap();
    let _lock = f.reporter.outbox_lock.lock().await;
    let retained = retain_with_credentials(
        &f.reporter,
        &admission,
        ProviderOutcome::NotStarted,
        None,
        Some(&ProviderFailureDiagnostic::new(
            ProviderFailureClass::LocalAdmission,
        )),
        &[],
    )
    .unwrap();
    let path = f.path(&retained.original);
    let bytes = std::fs::read(&path).unwrap();
    assert_eq!(
        read_report(&path, &f.reporter.config.limits).unwrap(),
        retained.original
    );
    assert_eq!(
        retained.original.report_id,
        contract::diagnostic_id("diagnostic-attempt", 1)
    );
    assert!(
        acknowledge(&f.reporter, &control, &retained)
            .await
            .unwrap_err()
            .contains("cancelled or expired")
    );
    assert_eq!(std::fs::read(path).unwrap(), bytes);
    assert!(f.script.lock().unwrap().requests.is_empty());
}

//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# A failed platform-owned provider request MUST retain bounded diagnostics sufficient to identify the observed failure class and available provider retry or quota information.
#[tokio::test]
async fn lost_ack_reconciles_original_report_once_after_historical_stop() {
    let mut f = fixture("lost-ack").await;
    let admission = f.admission(1);
    f.reporter.attempt.phase = AttemptPhase::Stopped;
    f.reporter.attempt.stopped = true;
    f.reporter.attempt.uncertain = true;
    // Currentness is not asserted by this scripted receipt seam: it proves the
    // consumer keeps the historical caller and asks no current-owner operation.
    let retained = retain_with_credentials(
        &f.reporter,
        &admission,
        ProviderOutcome::Unknown,
        Some(&binding()),
        Some(&unknown()),
        &[],
    )
    .unwrap();
    f.script.lock().unwrap().lose_next_ack = true;
    let (_cancel, control) = f.control();
    let context = acknowledge(&f.reporter, &control, &retained).await.unwrap();
    context
        .validate(
            &Operation::ObserveWorker {
                report: retained.original.clone(),
            },
            &Outcome::Ok {
                data: Data::Acknowledged,
            },
        )
        .unwrap();
    assert_eq!(f.sends(), 1);
    assert_eq!(
        f.script.lock().unwrap().original.as_ref(),
        Some(&retained.original)
    );
    assert!(!f.path(&retained.original).exists());
    let script = f.script.lock().unwrap();
    assert_eq!(script.requests.len(), 5);
    assert!(matches!(
        script.requests[0].command,
        Operation::GetOperation { .. }
    ));
    assert!(matches!(
        script.requests[4].command,
        Operation::GetOperation { .. }
    ));
    let operations: Vec<_> = script
        .requests
        .iter()
        .filter_map(|r| match &r.command {
            Operation::GetOperation { operation_id } => Some(operation_id.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(
        operations,
        vec![
            "diagnostic-attempt-native-request-1",
            "diagnostic-attempt-native-diagnostic-1",
            "diagnostic-attempt-native-request-1",
            "diagnostic-attempt-native-diagnostic-1"
        ]
    );
    assert_eq!(retained.original.incarnation, "original-incarnation");
    assert_eq!(retained.original.generation, 1);
}

#[tokio::test]
async fn elapsed_delivery_deadline_retains_observed_original_without_http() {
    let f = fixture("already-expired").await;
    let retained = retained(&f);
    let path = f.path(&retained.original);
    let bytes = std::fs::read(&path).unwrap();
    let (_sender, receiver) = watch::channel(false);
    let control = test_control(process::now_ms().saturating_sub(1), receiver);
    assert!(
        acknowledge(&f.reporter, &control, &retained)
            .await
            .unwrap_err()
            .contains("cancelled or expired")
    );
    assert_eq!(std::fs::read(path).unwrap(), bytes);
    assert!(f.script.lock().unwrap().requests.is_empty());
}

//= spec/impl/v2/implementation.md#platform-owned-model-retries
//= type=test
//# A failed platform-owned provider request MUST retain bounded diagnostics sufficient to identify the observed failure class and available provider retry or quota information.
#[tokio::test]
async fn cancelled_in_flight_ack_keeps_original_then_recovery_uses_lookup_only() {
    let f = fixture("cancel-in-flight").await;
    let retained = retained(&f);
    let path = f.path(&retained.original);
    let before = std::fs::read(&path).unwrap();
    let (entered, ready) = oneshot::channel();
    let (release, held) = oneshot::channel();
    f.script.lock().unwrap().hold_append = Some((entered, held));
    let (cancel, control) = f.control();
    let delivery = acknowledge(&f.reporter, &control, &retained);
    tokio::pin!(delivery);
    tokio::select! {
        result=&mut delivery=>panic!("delivery returned before controlled lost ACK: {result:?}"),
        result=ready=>result.unwrap(),
    }
    cancel.send(true).unwrap();
    assert!(delivery.await.unwrap_err().contains("cancelled or expired"));
    assert_eq!(std::fs::read(&path).unwrap(), before);
    assert_eq!(f.sends(), 1);
    release.send(()).unwrap();
    let (_cancel, control) = f.control();
    let context = acknowledge(&f.reporter, &control, &retained).await.unwrap();
    context
        .validate(
            &Operation::ObserveWorker {
                report: retained.original.clone(),
            },
            &Outcome::Ok {
                data: Data::Acknowledged,
            },
        )
        .unwrap();
    assert_eq!(f.sends(), 1);
    assert!(!path.exists());
}

#[tokio::test]
async fn mismatched_original_lookup_and_complete_refusal_keep_exact_outbox() {
    let f = fixture("wrong-original").await;
    let retained = retained(&f);
    let path = f.path(&retained.original);
    let bytes = std::fs::read(&path).unwrap();
    {
        let mut script = f.script.lock().unwrap();
        script.original = Some(retained.original.clone());
        script.wrong_original = true;
    }
    let (_cancel, control) = f.control();
    assert!(
        acknowledge(&f.reporter, &control, &retained)
            .await
            .unwrap_err()
            .contains("differs")
    );
    assert_eq!(f.sends(), 0);
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
    {
        let mut script = f.script.lock().unwrap();
        script.original = None;
        script.wrong_original = false;
        script.refuse_append = true;
    }
    assert!(
        acknowledge(&f.reporter, &control, &retained)
            .await
            .unwrap_err()
            .contains("refused")
    );
    assert_eq!(f.sends(), 1);
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
}

#[tokio::test]
async fn privacy_preserves_exact_nonsecret_quota_and_never_rewrites_original() {
    let f = fixture("privacy").await;
    let admission = f.admission(1);
    let mut facts = largest_facts(&admission.native);
    facts.class = ProviderFailureClass::HttpRefused;
    let http = facts.http.as_mut().unwrap();
    http.status = HttpStatusDiagnostic::Value(429);
    http.body = DiagnosticBodyState::Complete;
    http.category = CategoryDiagnostic::Value(ProviderErrorCategory::RateLimited);
    http.scheduler_retry_after = SchedulerRetryDiagnostic::Value(RetryAfter::Absent);
    http.requests_remaining = NumericDiagnostic::Value(8273619);
    http.quota = QuotaDiagnostic::Value(QuotaValues {
        dimension: QuotaDimension::TokensPerMinute,
        limit: "327680.125".into(),
        used: "327679.250".into(),
        requested: "1.875".into(),
        wait_ms: 1500,
    });
    let retained = retain_with_credentials(
        &f.reporter,
        &admission,
        ProviderOutcome::Rejected(ProviderRejection {
            kind: RejectionKind::RateLimited,
            retry_after: RetryAfter::Absent,
        }),
        Some(&binding()),
        Some(&facts),
        &["8273619".into()],
    )
    .unwrap();
    let Observation::NativeRequestDiagnostic {
        failure: Some(saved),
        ..
    } = &retained.original.observation
    else {
        panic!()
    };
    assert_eq!(
        saved.http.as_ref().unwrap().requests_remaining,
        NumericDiagnostic::Redacted
    );
    assert_eq!(
        saved.http.as_ref().unwrap().quota,
        facts.http.as_ref().unwrap().quota
    );
    let path = f.path(&retained.original);
    let before = std::fs::read(&path).unwrap();
    assert!(
        !String::from_utf8(before.clone())
            .unwrap()
            .contains("8273619")
    );
    assert!(
        retain_with_credentials(
            &f.reporter,
            &admission,
            ProviderOutcome::Completed,
            Some(&binding()),
            None,
            &[]
        )
        .err()
        .unwrap()
        .contains("changed body")
    );
    assert_eq!(std::fs::read(&path).unwrap(), before);
    let admission2 = f.admission(2);
    assert!(
        retain_with_credentials(
            &f.reporter,
            &admission2,
            ProviderOutcome::Unknown,
            Some(&binding()),
            Some(&unknown()),
            &["diagnostic-worker".into()]
        )
        .is_err()
    );
    assert!(!f.path(&admission2.original).exists());
    assert!(f.script.lock().unwrap().requests.is_empty());
}

#[tokio::test]
async fn failure_without_facts_binding_change_and_oversized_retained_body_are_refused() {
    let f = fixture("invalid-retained").await;
    let admission = f.admission(1);
    assert!(
        retain_with_credentials(
            &f.reporter,
            &admission,
            ProviderOutcome::NotStarted,
            None,
            Some(&largest_facts(&admission.native)),
            &[]
        )
        .is_err()
    );
    assert!(
        retain_with_credentials(
            &f.reporter,
            &admission,
            ProviderOutcome::Unknown,
            Some(&binding()),
            None,
            &[]
        )
        .is_err()
    );
    assert!(
        retain_with_credentials(
            &f.reporter,
            &admission,
            ProviderOutcome::Unknown,
            None,
            Some(&unknown()),
            &[]
        )
        .is_err()
    );
    let mut wrong = f.reporter.clone();
    wrong.config.incarnation = "replacement".into();
    assert!(
        retain_with_credentials(
            &wrong,
            &admission,
            ProviderOutcome::Unknown,
            Some(&binding()),
            Some(&unknown()),
            &[]
        )
        .err()
        .unwrap()
        .contains("admission differs")
    );
    let path = f.path(&admission.original);
    let bytes = vec![b'x'; f.reporter.config.limits.transport.body_bytes + 1];
    std::fs::write(&path, &bytes).unwrap();
    assert!(
        retain_with_credentials(
            &f.reporter,
            &admission,
            ProviderOutcome::Unknown,
            Some(&binding()),
            Some(&unknown()),
            &[]
        )
        .err()
        .unwrap()
        .contains("oversized local report")
    );
    assert_eq!(std::fs::read(path).unwrap(), bytes);
    assert!(f.script.lock().unwrap().requests.is_empty());
}

#[tokio::test]
async fn unstarted_waits_for_original_reservation_without_sending_or_claiming_commit() {
    let f = fixture("reservation-unknown").await;
    let retained = retain_with_credentials(
        &f.reporter,
        &f.admission(1),
        ProviderOutcome::NotStarted,
        None,
        Some(&ProviderFailureDiagnostic::new(
            ProviderFailureClass::LocalAdmission,
        )),
        &[],
    )
    .unwrap();
    let path = f.path(&retained.original);
    let bytes = std::fs::read(&path).unwrap();
    let (entered, ready) = oneshot::channel();
    let original = {
        let mut script = f.script.lock().unwrap();
        script.reservation_query = Some(entered);
        script.reservation.take().unwrap()
    };
    let (_cancel, control) = f.control();
    let delivery = acknowledge(&f.reporter, &control, &retained);
    tokio::pin!(delivery);
    tokio::select! {
        result=&mut delivery=>panic!("unknown reservation must hold delivery: {result:?}"),
        result=ready=>result.unwrap(),
    }
    assert_eq!(f.sends(), 0);
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
    assert!(f.script.lock().unwrap().requests.iter().all(
        |r| matches!(&r.command,Operation::GetOperation {operation_id}
            if operation_id==&contract::reservation_id("diagnostic-attempt",1))
    ));
    f.script.lock().unwrap().reservation = Some(original);
    let context = delivery.await.unwrap();
    context
        .validate(
            &Operation::ObserveWorker {
                report: retained.original.clone(),
            },
            &Outcome::Ok {
                data: Data::Acknowledged,
            },
        )
        .unwrap();
    assert_eq!(f.sends(), 1);
    assert_eq!(
        f.script.lock().unwrap().original.as_ref(),
        Some(&retained.original)
    );
    assert!(!path.exists());
}

#[tokio::test]
async fn cancelled_unknown_or_foreign_reservation_never_dispatches_diagnostic() {
    let f = fixture("reservation-cancel-foreign").await;
    let retained = retained(&f);
    let path = f.path(&retained.original);
    let bytes = std::fs::read(&path).unwrap();
    let (entered, ready) = oneshot::channel();
    let original = {
        let mut script = f.script.lock().unwrap();
        script.reservation_query = Some(entered);
        script.reservation.take().unwrap()
    };
    let (cancel, control) = f.control();
    let delivery = acknowledge(&f.reporter, &control, &retained);
    tokio::pin!(delivery);
    tokio::select! {
        result=&mut delivery=>panic!("unknown reservation must hold delivery: {result:?}"),
        result=ready=>result.unwrap(),
    }
    cancel.send(true).unwrap();
    assert!(delivery.await.unwrap_err().contains("cancelled or expired"));
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
    assert_eq!(f.sends(), 0);
    let mut foreign = original;
    foreign.1.id = "another-task".into();
    f.script.lock().unwrap().reservation = Some(foreign);
    let (_cancel, control) = f.control();
    assert!(
        acknowledge(&f.reporter, &control, &retained)
            .await
            .unwrap_err()
            .contains("outcome differs")
    );
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
    assert_eq!(f.sends(), 0);
}
