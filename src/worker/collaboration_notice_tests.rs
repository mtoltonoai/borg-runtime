use super::*;
use crate::collaboration::workflow::{
    ActivityDetail, ActorProvenance, AnswerRef, DeliveryScope, PrincipalRef,
    QuestionChange, QuestionResolution, RecordedActor, RecordRef, TrustedHostPath,
    WorkflowActivity, WorkflowContinuation, ResolvedWait, WaitRef, ResolverRef, ResolverOutcome,
};

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        Self(std::env::temp_dir().join(format!(
            "borg-collaboration-notice-{}-{}-{}",
            std::process::id(), process::now_ms(), NEXT.fetch_add(1, Ordering::Relaxed),
        )))
    }

    fn host(&self, caller: &Caller) -> Host {
        self.with_policy(caller, policy())
    }

    fn with_policy(&self, caller: &Caller, policy: WorkflowLimits) -> Host {
        let mut runtime = (*crate::worker::test_limits()).clone();
        runtime.workflow = Some(policy.clone());
        let mut input = TaskInput { instruction_source: None,
            pull_request: None,
            resolved_limits: None, repository_read: None,
            instruction: "continue the accepted collaboration task".into(),
            definition: Definition {
                charter: None,
                limit_overrides: None,
                execution_profile: Some(ExecutionProfile::CollaborationV2 { publication: None, web_search: None }),
                inference: Some(InferenceSettings::selected()),
                id: "agent-1".into(), revision: "agent-revision-1".into(),
                executor: ExecutorKind::Native,
            },
            repository: None, acceptance_commands: vec![],
            output_schema: Some(json!({"type":"object"})),
            limits: runtime.task_defaults.at(1).unwrap(),
            dependencies: vec![], native_workflow: None,
        };
        input.resolved_limits = Some(runtime.resolve_task(&input).unwrap());
        input.execution_limits().unwrap();
        let accepted = input.resolved_limits.as_ref().unwrap().global_metadata.as_ref().clone();
        Host::open_with_credentials(
            ApiClient::new("http://127.0.0.1:1", Arc::new(accepted)).unwrap(),
            &self.0, caller, "session-1", "agent-1", &policy, vec![],
        ).unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) { let _ = std::fs::remove_dir_all(&self.0); }
}

fn policy() -> WorkflowLimits {
    WorkflowLimits {
        record_bytes: 65_536, text_bytes: 4096, options: 8, records: 128,
        revisions: 1024, page_items: 8, page_bytes: 65_536, waits_per_task: 8,
        graph_scan_nodes: 256, events_per_mutation: 16, event_bytes: 16_384,
        notice_bytes: 16_384, pending_delivery_bytes: 131_072, delivery_scan_rows: 32,
    }
}

fn caller() -> Caller {
    Caller { task_id: "reader-task".into(), attempt_id: "attempt-1".into(),
        generation: 1, worker_id: "worker-1".into(), incarnation: "incarnation-1".into() }
}

fn next_caller() -> Caller {
    Caller { task_id: "reader-task".into(), attempt_id: "attempt-2".into(),
        generation: 2, worker_id: "worker-2".into(), incarnation: "incarnation-2".into() }
}

fn event(revision: u64, ordinal: u32) -> EventId {
    EventId { database_id: "database-1".into(), product_revision: revision, ordinal }
}

fn cursor(position: Option<EventId>) -> DeliveryCursor {
    DeliveryCursor { database_id: "database-1".into(),
        scope: DeliveryScope { task_id: "reader-task".into(), session_id: "session-1".into(), agent_id: "agent-1".into() },
        position }
}

fn activity() -> WorkflowActivity {
    WorkflowActivity {
        id: event(11, 1), subject: RecordRef::Question { question_id: "question-1".into() },
        subject_revision: 2,
        actor: RecordedActor { principal: PrincipalRef::TrustedHostOperator,
            provenance: ActorProvenance::TrustedHost { path: TrustedHostPath::Browser } },
        detail: ActivityDetail::Question(QuestionChange::Closed {
            resolution: QuestionResolution::Answered { question_id: "question-1".into(),
                answer: AnswerRef { question_id: "question-1".into(), answer_id: "answer-2".into() },
                selected_option_id: "option-1".into() },
        }),
        truncated: false, routing: None,
    }
}

fn observation() -> DeliveryObservation {
    DeliveryObservation { routing_cursor: None, routing_turn: false, superseded: None,
        acknowledged_cursor: cursor(Some(event(10, 0))),
        pending: Some(DeliveryBatch {
            batch_id: "original-prepare-1".into(), prepared_by: caller(), delivery: None,
            page: DeliveryPage { routing: None, from_cursor: cursor(Some(event(10, 0))),
                to_cursor: cursor(Some(event(11, 2))), through: event(12, 0),
                items: vec![activity()], continuation: None, has_more: true },
        }),
        available: None,
    }
}

fn intent(caller: Caller, context: &str) -> DeliveryIntent {
    DeliveryIntent { operation_id: "original-begin-1".into(), caller,
        context_binding: context.into() }
}

fn with_intent(mut observation: DeliveryObservation, intent: &DeliveryIntent) -> DeliveryObservation {
    observation.pending.as_mut().unwrap().delivery = Some(intent.clone());
    observation
}

fn continuation() -> WorkflowContinuation {
    WorkflowContinuation {
        task_id: "reader-task".into(), session_id: "session-1".into(),
        resolved_waits: vec![ResolvedWait {
            wait: WaitRef { task_id: "reader-task".into(), wait_id: "wait-question-1".into() },
            resolver: ResolverRef::Question { question_id: "question-1".into() },
            event_id: event(11, 1),
            outcome: ResolverOutcome::Question(QuestionResolution::Answered {
                question_id: "question-1".into(),
                answer: AnswerRef { question_id: "question-1".into(), answer_id: "answer-2".into() },
                selected_option_id: "option-1".into(),
            }),
        }],
    }
}

fn replacement_offer() -> DeliveryObservation {
    let mut observed = observation();
    let mut available = observed.pending.as_ref().unwrap().page.clone();
    available.continuation = Some(continuation());
    observed.available = Some(available);
    observed
}

fn replacement_receipt() -> (Operation, DeliveryObservation) {
    let offered = replacement_offer();
    let page = offered.available.unwrap();
    let request = Operation::PrepareCollaborationDelivery { expected_routing_cursor: None,
        operation_id: "replacement-prepare-2".into(), caller: next_caller(),
        expected_cursor: page.from_cursor.clone(), through: page.through.clone(), limit: 1,
    };
    (request, DeliveryObservation { routing_cursor: None, routing_turn: false, superseded: None,
        acknowledged_cursor: offered.acknowledged_cursor,
        pending: Some(DeliveryBatch { batch_id: "replacement-prepare-2".into(),
            prepared_by: next_caller(), delivery: None, page }), available: None,
    })
}

fn retain_outcome(host: &Host, name: &str, request: Operation, observation: DeliveryObservation) {
    process::private_directory(&host.directory).unwrap();
    let path = host.directory.join(name);
    let operation_id = mutation_identity(&request).unwrap().0.to_owned();
    let mut journal = mutations::create(&path, request, 0, host.client.limits()).unwrap();
    journal.outcome = Some(Outcome::Ok { data: Data::CollaborationDelivery(Box::new(observation)) });
    journal.outcome_context = Some(CommitContext {
        commit: MutationCommit { operation_id, controller_revision: 17 },
        observed_controller_revision: 17,
    });
    mutations::persist(&path, &journal).unwrap();
}

fn control() -> (watch::Sender<bool>, Control) {
    let (sender, receiver) = watch::channel(false);
    (sender, crate::worker::test_control(process::now_ms() + 60_000, receiver))
}

#[test]
fn absent_workflow_policy_cannot_open_delivery_and_empty_history_creates_no_files() {
    let f = Fixture::new();
    let mut disabled = (*crate::worker::test_limits()).clone();
    disabled.workflow = None;
    assert!(Host::open_with_credentials(
        ApiClient::new("http://127.0.0.1:1", Arc::new(disabled)).unwrap(),
        &f.0, &caller(), "session-1", "agent-1", &policy(), vec![],
    ).is_err());
    let mut host = f.host(&caller());
    host.accept(&DeliveryObservation { routing_cursor: None, routing_turn: false, superseded: None, acknowledged_cursor: cursor(None), pending: None, available: None }).unwrap();
    assert!(!f.0.exists());
}

#[test]
fn changed_scope_event_order_and_unacknowledged_batch_never_rewrite_mirror() {
    let f = Fixture::new();
    let mut host = f.host(&caller());
    host.accept(&observation()).unwrap();
    let path = host.directory.join("delivery.json");
    let original = std::fs::read(&path).unwrap();
    for mode in ["database", "task", "session", "agent", "pending", "events", "through"] {
        let mut changed = observation();
        match mode {
            "database" => changed.acknowledged_cursor.database_id = "other-database".into(),
            "task" => changed.acknowledged_cursor.scope.task_id = "other-task".into(),
            "session" => changed.acknowledged_cursor.scope.session_id = "other-session".into(),
            "agent" => changed.acknowledged_cursor.scope.agent_id = "other-agent".into(),
            "pending" => changed.pending = None,
            "events" => changed.pending.as_mut().unwrap().page.items.push(activity()),
            _ => changed.pending.as_mut().unwrap().page.through = event(9, 0),
        }
        assert!(host.accept(&changed).is_err(), "accepted {mode}");
        assert_eq!(std::fs::read(&path).unwrap(), original);
    }
}

#[test]
fn exact_event_refs_survive_encoding_and_first_event_must_fit_before_prepare() {
    let f = Fixture::new();
    let host = f.host(&caller());
    let batch = observation().pending.unwrap();
    let text = host.encode(&batch).unwrap();
    let value: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(value["events"], serde_json::to_value(&batch.page.items).unwrap());
    assert_eq!(value["batch_id"], batch.batch_id);
    let exact = serde_json::to_vec(&text).unwrap().len();
    let mut selected = policy();
    selected.notice_bytes = exact;
    let fitting = f.with_policy(&caller(), selected.clone());
    assert_eq!(fitting.fitting_limit(&batch.page, &batch.batch_id).unwrap(), 1);
    selected.notice_bytes -= 1;
    let refused = f.with_policy(&caller(), selected);
    assert!(refused.fitting_limit(&batch.page, &batch.batch_id).is_err());
    assert!(!f.0.exists(), "fitting must not prepare, acknowledge, or advance any cursor");
}

#[test]
fn begin_reserves_exact_intent_and_local_marker_before_any_mutation() {
    let f = Fixture::new();
    let mut host = f.host(&caller());
    host.accept(&observation()).unwrap();
    let intent = intent(caller(), "thread-1-turn-2");
    let mut candidate = host.append_candidate("original-prepare-1", &intent).unwrap();
    candidate.limits.notice_bytes = serde_json::to_vec(&candidate.pending.as_ref().unwrap().text).unwrap().len();
    // Include the selected numeric limits themselves in the exact byte count.
    candidate.accepted_limits.workflow = Some(candidate.limits.clone());
    for _ in 0..3 {
        candidate.limits.pending_delivery_bytes = serde_json::to_vec(&candidate).unwrap().len();
        candidate.accepted_limits.workflow = Some(candidate.limits.clone());
    }
    let bound = serde_json::to_vec(&candidate).unwrap().len();
    assert_eq!(candidate.limits.pending_delivery_bytes, bound);
    let fit = Fixture::new();
    let mut fitting = fit.with_policy(&caller(), candidate.limits.clone());
    fitting.accept(&observation()).unwrap();
    assert!(fitting.append_candidate("original-prepare-1", &intent).is_ok());
    candidate.limits.pending_delivery_bytes -= 1;
    let refused = Fixture::new();
    let mut refusing = refused.with_policy(&caller(), candidate.limits);
    refusing.accept(&observation()).unwrap();
    let original = std::fs::read(refusing.directory.join("delivery.json")).unwrap();
    assert!(refusing.append_candidate("original-prepare-1", &intent).is_err());
    assert_eq!(std::fs::read(refusing.directory.join("delivery.json")).unwrap(), original);
    assert!(!refusing.directory.join("begin.json").exists());
}

#[tokio::test]
async fn original_begin_receipt_recovers_before_marker_but_unknown_append_never_resends() {
    let f = Fixture::new();
    let intent = intent(caller(), "thread-1-turn-2");
    let observed = with_intent(observation(), &intent);
    let mut host = f.host(&caller());
    host.accept(&observation()).unwrap();
    retain_outcome(&host, "begin.json", Operation::BeginCollaborationDelivery {
        operation_id: intent.operation_id.clone(), caller: caller(),
        batch_id: "original-prepare-1".into(), context_binding: intent.context_binding.clone(),
    }, observed.clone());
    drop(host);
    let mut recovered = f.host(&caller());
    recovered.accept(&observed).unwrap();
    recovered.current_confirmed = true;
    let (_sender, control) = control();
    assert_eq!(recovered.begin_append("original-prepare-1", "thread-1-turn-2", &control).await.unwrap(), AppendDisposition::Ready);
    let original = std::fs::read(recovered.directory.join("delivery.json")).unwrap();
    assert!(recovered.read_mutation(&recovered.directory.join("begin.json")).unwrap().unwrap().consumed);
    drop(recovered);
    for caller in [caller(), next_caller()] {
        let mut reopened = f.host(&caller);
        reopened.accept(&observed).unwrap();
        reopened.current_confirmed = true;
        for context in ["thread-1-turn-2", "different-context"] {
            assert_eq!(reopened.begin_append("original-prepare-1", context, &control).await.unwrap(), AppendDisposition::Deferred);
            assert_eq!(std::fs::read(reopened.directory.join("delivery.json")).unwrap(), original);
        }
    }
}

#[tokio::test]
async fn inherited_preintent_can_be_reserved_but_canonical_intent_without_journal_defers() {
    let f = Fixture::new();
    let mut host = f.host(&next_caller());
    host.accept(&observation()).unwrap();
    assert!(host.append_candidate("original-prepare-1", &intent(next_caller(), "new-context")).is_ok());
    let observed = with_intent(observation(), &intent(caller(), "old-context"));
    host.accept(&observed).unwrap();
    drop(host);
    let (_sender, control) = control();
    for _ in 0..2 {
        let mut reopened = f.host(&next_caller());
        reopened.accept(&observed).unwrap();
        reopened.current_confirmed = true;
        assert_eq!(reopened.begin_append("original-prepare-1", "new-context", &control).await.unwrap(), AppendDisposition::Deferred);
        assert!(!reopened.directory.join("begin.json").exists());
    }
}

#[test]
fn append_confirmation_requires_exact_context_and_canonical_intent_cannot_change() {
    let f = Fixture::new();
    let mut host = f.host(&caller());
    let original_intent = intent(caller(), "thread-1-turn-2");
    let observed = with_intent(observation(), &original_intent);
    host.accept(&observed).unwrap();
    host.mark_begin("original-prepare-1", &original_intent).unwrap();
    let original = std::fs::read(host.directory.join("delivery.json")).unwrap();
    assert!(host.mark_appended("original-prepare-1", "wrong-context").is_err());
    assert_eq!(std::fs::read(host.directory.join("delivery.json")).unwrap(), original);
    assert!(host.accept(&observation()).is_err());
    let changed = with_intent(observation(), &intent(next_caller(), "other-context"));
    assert!(host.accept(&changed).is_err());
    host.mark_appended("original-prepare-1", "thread-1-turn-2").unwrap();
    assert!(host.mirror.unwrap().pending.unwrap().delivery.unwrap().appended);
}

#[tokio::test]
async fn original_ack_receipt_retires_unknown_local_append_without_resending_input() {
    let f = Fixture::new();
    let intent = intent(caller(), "thread-1-turn-2");
    let observed = with_intent(observation(), &intent);
    let mut host = f.host(&caller());
    host.accept(&observed).unwrap();
    host.mark_begin("original-prepare-1", &intent).unwrap();
    // Crash can leave the local marker uncertain while the original ACK has a
    // durable success receipt. Recovery must resolve it independently of send.
    let acknowledged = DeliveryObservation { routing_cursor: None, routing_turn: false, superseded: None, acknowledged_cursor: observed.pending.unwrap().page.to_cursor,
        pending: None, available: None };
    retain_outcome(&host, "acknowledge.json", Operation::AcknowledgeCollaborationDelivery {
        operation_id: "original-ack-1".into(), caller: caller(),
        batch_id: "original-prepare-1".into(), context_binding: intent.context_binding,
    }, acknowledged.clone());
    drop(host);
    let mut reopened = f.host(&next_caller());
    let (_sender, control) = control();
    reopened.retire_ack(&control).await.unwrap();
    assert_eq!(reopened.mirror.as_ref().unwrap().acknowledged_cursor, acknowledged.acknowledged_cursor);
    assert!(reopened.mirror.as_ref().unwrap().pending.is_none());
    assert!(reopened.read_mutation(&reopened.directory.join("acknowledge.json")).unwrap().unwrap().consumed);
}

#[test]
fn empty_prefix_has_no_model_input_and_scan_exhaustion_is_typed_deferral() {
    let f = Fixture::new();
    let host = f.host(&caller());
    let mut batch = observation().pending.unwrap();
    batch.page.items.clear();
    assert_eq!(host.encode(&batch).unwrap(), "");
    assert_eq!(prefix_context(&batch), "original-prepare-1");
    assert_eq!(completed_range(true), PrepareDisposition::Deferred);
    assert_eq!(completed_range(false), PrepareDisposition::NoChanges);
    assert_eq!(batch.page.to_cursor.position, Some(event(11, 2)));
    assert_ne!(batch.page.to_cursor.position, Some(batch.page.through.clone()));
    batch.batch_id = "x".repeat(host.client.limits().transport.identity_bytes);
    assert_eq!(prefix_context(&batch).len(), host.client.limits().transport.identity_bytes);
    assert!(host.fitting_limit(&batch.page, &batch.batch_id).is_ok());
}

#[test]
fn sequential_resolved_waits_accumulate_past_open_wait_limit_without_losing_exact_refs() {
    let f = Fixture::new();
    let mut selected = policy();
    selected.waits_per_task = 1;
    selected.revisions = 2;
    let host = f.with_policy(&caller(), selected.clone());
    let mut accumulated = continuation();
    // Both waits have already resolved. They represent sequential batches
    // admitted with at most one open wait, still owed to the same session.
    accumulated.resolved_waits.push(ResolvedWait {
        wait: WaitRef { task_id: "reader-task".into(), wait_id: "wait-question-2".into() },
        resolver: ResolverRef::Question { question_id: "question-2".into() },
        event_id: event(11, 2),
        outcome: ResolverOutcome::Question(QuestionResolution::Answered {
            question_id: "question-2".into(),
            answer: AnswerRef { question_id: "question-2".into(), answer_id: "answer-3".into() },
            selected_option_id: "option-2".into(),
        }),
    });
    assert!(accumulated.resolved_waits.len() > selected.waits_per_task);
    let mut batch = observation().pending.unwrap();
    batch.page.items.clear();
    batch.page.continuation = Some(accumulated.clone());
    let text = host.encode(&batch).unwrap();
    let rendered: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(rendered["continuation"], serde_json::to_value(&accumulated).unwrap());
    assert_eq!(host.fitting_limit(&batch.page, &batch.batch_id).unwrap(), selected.page_items);

    let mut fewer_revisions = selected.clone();
    fewer_revisions.revisions = 1;
    assert!(f.with_policy(&caller(), fewer_revisions).encode(&batch).unwrap_err().contains("workflow.revisions"));

    // Byte admission still rejects the complete continuation; it cannot make
    // the payload fit by dropping an earlier resolved wait.
    selected.notice_bytes = serde_json::to_vec(&text).unwrap().len() - 1;
    let bounded = f.with_policy(&caller(), selected);
    assert!(bounded.encode(&batch).unwrap_err().contains("workflow.notice_bytes"));
    assert_eq!(batch.page.continuation, Some(accumulated));
    assert!(!f.0.exists(), "validation must not append, ACK or advance the cursor");
}

#[test]
fn separately_bounded_pages_pass_observation_and_fitting_before_either_event_is_dropped() {
    let f = Fixture::new();
    let mut first = observation().pending.unwrap().page;
    first.to_cursor.position = Some(first.items[0].id.clone());
    let mut second = first.clone();
    second.from_cursor = first.to_cursor.clone();
    second.items[0].id = second.through.clone();
    second.to_cursor.position = Some(second.through.clone());
    second.has_more = false;
    let mut combined = first.clone();
    combined.items.push(second.items[0].clone());
    combined.to_cursor = second.to_cursor.clone();
    combined.has_more = false;
    let mut selected = policy();
    selected.page_bytes = serde_json::to_vec(&first).unwrap().len()
        .max(serde_json::to_vec(&second).unwrap().len());
    let combined_bytes = serde_json::to_vec(&combined).unwrap().len();
    assert!(combined_bytes > selected.page_bytes && combined_bytes < selected.pending_delivery_bytes);
    let host = f.with_policy(&caller(), selected);
    let observation = |page: DeliveryPage| DeliveryObservation { routing_cursor: None, routing_turn: false, superseded: None,
        acknowledged_cursor: page.from_cursor.clone(), pending: None, available: Some(page),
    };
    assert!(host.validate_observation(&observation(combined)).unwrap_err().contains("workflow.page_bytes"));
    for (index, page) in [first, second].into_iter().enumerate() {
        host.validate_observation(&observation(page.clone())).unwrap();
        let batch_id = format!("page-prepare-{index}");
        assert_eq!(host.fitting_limit(&page, &batch_id).unwrap(), 1);
        let text = host.encode(&DeliveryBatch {
            batch_id, prepared_by: caller(), delivery: None, page: page.clone(),
        }).unwrap();
        let rendered: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(rendered["events"], serde_json::to_value(&page.items).unwrap());
    }
    assert!(!f.0.exists(), "fitting does not append, ACK or skip a page");
}

#[test]
fn fenced_absent_prepare_and_begin_keep_exact_evidence_across_interrupted_retirement() {
    for slot in ["prepare", "begin"] {
        let f = Fixture::new();
        let mut host = f.host(&next_caller());
        let mut observed = observation();
        if slot == "prepare" {
            observed.available = Some(observed.pending.take().unwrap().page);
        }
        host.accept(&observed).unwrap();
        host.current_confirmed = true;
        process::private_directory(&host.directory).unwrap();
        let request = if slot == "prepare" {
            Operation::PrepareCollaborationDelivery { expected_routing_cursor: None,
                operation_id: "old-absent-prepare".into(), caller: caller(),
                expected_cursor: observed.acknowledged_cursor.clone(), through: event(12, 0), limit: 1,
            }
        } else {
            Operation::BeginCollaborationDelivery {
                operation_id: "old-absent-begin".into(), caller: caller(),
                batch_id: "original-prepare-1".into(), context_binding: "old-context".into(),
            }
        };
        let path = host.directory.join(format!("{slot}.json"));
        let journal = mutations::create(&path, request, 0, host.client.limits()).unwrap();
        let original = std::fs::read(&path).unwrap();
        let archive = host.archive_directory(slot, mutation_identity(&journal.request).unwrap().0);
        // The typed-absence seam is exercised after the caller fence and exact
        // preintent observation. Cut after moving the exact old bytes, before
        // publishing completed retirement or allowing a replacement request.
        let fault = process::storage_tests::inject(&archive.join("retirement.json"),
            process::storage_tests::Point::Create, 28, 0);
        assert!(host.retire_absent(&journal).is_err());
        assert_eq!(fault.hits(), 1);
        assert!(!path.exists());
        assert_eq!(std::fs::read(archive.join("original.json")).unwrap(), original);
        assert!(unresolved(&f.0, host.client.limits()).unwrap_or(true));
        drop(host);
        let mut reopened = f.host(&next_caller());
        reopened.accept(&observed).unwrap();
        reopened.current_confirmed = true;
        reopened.finish_retirement(slot).unwrap();
        assert_eq!(std::fs::read(archive.join("original.json")).unwrap(), original);
        assert!(reopened.read_mutation(&archive.join("original.json")).unwrap().unwrap().outcome.is_none());
        assert!(audit_retirement(&reopened.retirement_path(slot), reopened.client.limits()).unwrap().complete);
        assert!(!path.exists());
        assert!(!unresolved(&f.0, reopened.client.limits()).unwrap());
        assert!(reopened.mirror.as_ref().unwrap().pending.as_ref().is_none_or(|pending| pending.batch.delivery.is_none()));
    }
}

#[test]
fn canonical_intent_forbids_retirement_even_with_an_old_unresolved_local_begin() {
    let f = Fixture::new();
    let mut host = f.host(&next_caller());
    let original_intent = intent(caller(), "old-context");
    host.accept(&with_intent(observation(), &original_intent)).unwrap();
    host.current_confirmed = true;
    let path = host.directory.join("begin.json");
    let journal = mutations::create(&path, Operation::BeginCollaborationDelivery {
        operation_id: original_intent.operation_id.clone(), caller: caller(),
        batch_id: "original-prepare-1".into(), context_binding: "old-context".into(),
    }, 0, host.client.limits()).unwrap();
    let original = std::fs::read(&path).unwrap();
    assert!(host.retire_absent(&journal).is_err());
    assert_eq!(std::fs::read(&path).unwrap(), original);
    assert!(!host.retirement_path("begin").exists());
    assert!(!host.archive_directory("begin", &original_intent.operation_id).exists());
    assert!(unresolved(&f.0, host.client.limits()).unwrap());
}

#[test]
fn local_uncertainty_audit_does_not_close_unknown_append_or_hold_a_later_exact_ack() {
    let f = Fixture::new();
    let mut host = f.host(&caller());
    host.accept(&observation()).unwrap();
    assert!(!unresolved(&f.0, host.client.limits()).unwrap());
    let mut later_startup = host.client.limits().clone();
    later_startup.revision = "later-worker-startup".into();
    later_startup.workflow = None;
    assert!(!unresolved(&f.0, &later_startup).unwrap(), "historical mirror keeps its accepted enabled policy");
    let original_intent = intent(caller(), "context-1");
    let observed = with_intent(observation(), &original_intent);
    retain_outcome(&host, "begin.json", Operation::BeginCollaborationDelivery {
        operation_id: original_intent.operation_id.clone(), caller: caller(),
        batch_id: "original-prepare-1".into(), context_binding: "context-1".into(),
    }, observed.clone());
    host.accept(&observed).unwrap();
    host.mark_begin("original-prepare-1", &original_intent).unwrap();
    assert!(unresolved(&f.0, host.client.limits()).unwrap());
    assert!(unresolved(&f.0, &later_startup).unwrap());
    let acknowledged = DeliveryObservation { routing_cursor: None, routing_turn: false, superseded: None,
        acknowledged_cursor: observed.pending.unwrap().page.to_cursor, pending: None, available: None,
    };
    retain_outcome(&host, "acknowledge.json", Operation::AcknowledgeCollaborationDelivery {
        operation_id: "exact-ack-1".into(), caller: caller(),
        batch_id: "original-prepare-1".into(), context_binding: "context-1".into(),
    }, acknowledged.clone());
    host.accept(&acknowledged).unwrap();
    assert!(!unresolved(&f.0, host.client.limits()).unwrap(), "later exact cursor closes original receipts even before consume");
    assert!(!unresolved(&f.0, &later_startup).unwrap(), "startup metadata does not reinterpret closed accepted journals");
    std::fs::write(host.directory.join("begin.json"), b"broken evidence").unwrap();
    assert!(unresolved(&f.0, host.client.limits()).is_err(), "malformed evidence must retain cleanup uncertainty");
}

#[tokio::test]
async fn cancellation_before_begin_never_claims_or_appends_the_prepared_batch() {
    let f = Fixture::new();
    let mut host = f.host(&caller());
    host.accept(&observation()).unwrap();
    host.current_confirmed = true;
    let original = std::fs::read(host.directory.join("delivery.json")).unwrap();
    let (sender, control) = control();
    sender.send(true).unwrap();
    assert!(host.begin_append("original-prepare-1", "context-1", &control).await.is_err());
    assert!(!host.directory.join("begin.json").exists());
    assert_eq!(std::fs::read(host.directory.join("delivery.json")).unwrap(), original);
}

#[tokio::test]
async fn continuation_only_batch_appends_exact_resolvers_and_same_cursor_is_not_an_ack() {
    let f = Fixture::new();
    let mut host = f.host(&caller());
    let mut observed = observation();
    let batch = observed.pending.as_mut().unwrap();
    batch.page.items.clear();
    batch.page.continuation = Some(continuation());
    batch.page.from_cursor = batch.page.to_cursor.clone();
    batch.page.through = batch.page.to_cursor.position.clone().unwrap();
    batch.page.has_more = false;
    observed.acknowledged_cursor = batch.page.from_cursor.clone();
    let text = host.encode(batch).unwrap();
    assert!(!text.is_empty());
    let encoded: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(encoded["events"], json!([]));
    assert_eq!(encoded["continuation"], serde_json::to_value(continuation()).unwrap());
    host.accept(&observed).unwrap();
    retain_outcome(&host, "prepare.json", Operation::PrepareCollaborationDelivery { expected_routing_cursor: None,
        operation_id: "original-prepare-1".into(), caller: caller(),
        expected_cursor: observed.acknowledged_cursor.clone(),
        through: observed.acknowledged_cursor.position.clone().unwrap(), limit: 1,
    }, observed.clone());
    let (_sender, control) = control();
    host.recover_prepare(&control, false).await.unwrap();
    assert!(host.read_mutation(&host.directory.join("prepare.json")).unwrap().unwrap().consumed);
    let original_intent = intent(caller(), "continuation-context");
    let begun = with_intent(observed, &original_intent);
    retain_outcome(&host, "begin.json", Operation::BeginCollaborationDelivery {
        operation_id: original_intent.operation_id.clone(), caller: caller(),
        batch_id: "original-prepare-1".into(), context_binding: original_intent.context_binding.clone(),
    }, begun.clone());
    assert!(unresolved(&f.0, host.client.limits()).unwrap(), "unchanged cursor cannot close the Begin receipt");
    host.accept(&begun).unwrap();
    host.mark_begin("original-prepare-1", &original_intent).unwrap();
    assert!(host.acknowledge(&control).await.is_err(), "continuation is real input, never an empty-prefix ACK");
    assert!(!host.directory.join("acknowledge.json").exists());
    let acknowledged = DeliveryObservation { routing_cursor: None, routing_turn: false, superseded: None,
        acknowledged_cursor: begun.acknowledged_cursor, pending: None, available: None,
    };
    retain_outcome(&host, "acknowledge.json", Operation::AcknowledgeCollaborationDelivery {
        operation_id: "continuation-ack".into(), caller: caller(),
        batch_id: "original-prepare-1".into(), context_binding: original_intent.context_binding,
    }, acknowledged.clone());
    host.accept(&acknowledged).unwrap();
    assert!(!unresolved(&f.0, host.client.limits()).unwrap());
}

#[tokio::test]
async fn initial_cursor_continuation_reopens_and_only_the_original_ack_closes_append_uncertainty() {
    let f = Fixture::new();
    let mut host = f.host(&caller());
    let mut observed = observation();
    observed.acknowledged_cursor = cursor(None);
    let batch = observed.pending.as_mut().unwrap();
    batch.page.from_cursor = cursor(None);
    batch.page.to_cursor = cursor(None);
    batch.page.items.clear();
    batch.page.continuation = Some(continuation());
    batch.page.has_more = true;
    let expected_text = host.encode(batch).unwrap();
    let rendered: serde_json::Value = serde_json::from_str(&expected_text).unwrap();
    assert_eq!(rendered["continuation"], serde_json::to_value(continuation()).unwrap());
    let original_prepare = Operation::PrepareCollaborationDelivery { expected_routing_cursor: None,
        operation_id: batch.batch_id.clone(), caller: caller(), expected_cursor: cursor(None),
        through: batch.page.through.clone(), limit: 1,
    };
    host.accept(&observed).unwrap();
    retain_outcome(&host, "prepare.json", original_prepare, observed.clone());
    drop(host);
    let mut host = f.host(&caller());
    let (_sender, control) = control();
    host.recover_prepare(&control, false).await.unwrap();
    assert_eq!(host.mirror.as_ref().unwrap().pending.as_ref().unwrap().text, expected_text);
    assert_eq!(host.mirror.as_ref().unwrap().acknowledged_cursor.position, None);
    let original_intent = intent(caller(), "initial-cursor-context");
    let begun = with_intent(observed, &original_intent);
    retain_outcome(&host, "begin.json", Operation::BeginCollaborationDelivery {
        operation_id: original_intent.operation_id.clone(), caller: caller(),
        batch_id: "original-prepare-1".into(), context_binding: original_intent.context_binding.clone(),
    }, begun.clone());
    host.accept(&begun).unwrap();
    host.mark_begin("original-prepare-1", &original_intent).unwrap();
    assert!(host.acknowledge(&control).await.is_err(), "required continuation is never an empty-prefix ACK");
    drop(host);
    let mut host = f.host(&caller());
    assert!(unresolved(&f.0, host.client.limits()).unwrap());
    host.accept(&begun).unwrap();
    host.current_confirmed = true;
    assert_eq!(host.begin_append("original-prepare-1", &original_intent.context_binding, &control).await.unwrap(), AppendDisposition::Deferred);
    let acknowledged = DeliveryObservation { routing_cursor: None, routing_turn: false, superseded: None, acknowledged_cursor: cursor(None), pending: None, available: None };
    retain_outcome(&host, "acknowledge.json", Operation::AcknowledgeCollaborationDelivery {
        operation_id: "initial-cursor-original-ack".into(), caller: caller(),
        batch_id: "original-prepare-1".into(), context_binding: original_intent.context_binding,
    }, acknowledged.clone());
    host.accept(&acknowledged).unwrap();
    drop(host);
    let host = f.host(&caller());
    assert!(host.mirror.as_ref().unwrap().pending.is_none());
    assert_eq!(host.mirror.as_ref().unwrap().acknowledged_cursor.position, None);
    assert!(!unresolved(&f.0, host.client.limits()).unwrap());
}

#[tokio::test]
async fn lost_replacement_prepare_recovers_exact_batch_without_acknowledging_old_cursor() {
    let f = Fixture::new();
    let mut host = f.host(&next_caller());
    let offer = replacement_offer();
    host.accept(&offer).unwrap();
    let (request, replacement) = replacement_receipt();
    retain_outcome(&host, "prepare.json", request, replacement.clone());
    drop(host);
    let mut reopened = f.host(&next_caller());
    let (_sender, control) = control();
    reopened.recover_prepare(&control, false).await.unwrap();
    assert_eq!(reopened.mirror.as_ref().unwrap().acknowledged_cursor, offer.acknowledged_cursor);
    assert_eq!(reopened.mirror.as_ref().unwrap().pending.as_ref().unwrap().batch, replacement.pending.unwrap());
    assert!(reopened.read_mutation(&reopened.directory.join("prepare.json")).unwrap().unwrap().consumed);
    assert!(!reopened.directory.join("acknowledge.json").exists());
    let files = bounded_inventory(&reopened.directory, &reopened.client.limits().worker).unwrap();
    let archives: Vec<_> = files.iter().filter(|path| path.file_name().and_then(|name| name.to_str()) == Some("supersession.json")).collect();
    assert_eq!(archives.len(), 1);
    let archived: Supersession = serde_json::from_slice(&std::fs::read(archives[0]).unwrap()).unwrap();
    assert_eq!(archived.previous.pending.unwrap().batch, offer.pending.unwrap());
    assert!(!unresolved(&f.0, reopened.client.limits()).unwrap());
}

#[tokio::test]
async fn original_begin_wins_replacement_and_new_prepare_wins_only_after_fenced_absence() {
    let f = Fixture::new();
    let mut host = f.host(&next_caller());
    host.accept(&replacement_offer()).unwrap();
    host.current_confirmed = true;
    let original_intent = intent(caller(), "old-context");
    let request = Operation::BeginCollaborationDelivery {
        operation_id: original_intent.operation_id.clone(), caller: caller(),
        batch_id: "original-prepare-1".into(), context_binding: original_intent.context_binding.clone(),
    };
    retain_outcome(&host, "begin.json", request.clone(), with_intent(observation(), &original_intent));
    let (_sender, control) = control();
    assert!(host.settle_replacement_predecessor(&control).await.is_err());
    assert_eq!(host.mirror.as_ref().unwrap().pending.as_ref().unwrap().batch.delivery, Some(original_intent));
    assert!(!host.directory.join("prepare.json").exists());
    assert!(!host.directory.join("acknowledge.json").exists());

    let f = Fixture::new();
    let mut host = f.host(&next_caller());
    host.accept(&replacement_offer()).unwrap();
    host.current_confirmed = true;
    let journal = mutations::create(&host.directory.join("begin.json"), request, 0, host.client.limits()).unwrap();
    host.retire_absent(&journal).unwrap();
    let (request, replacement) = replacement_receipt();
    host.accept_prepared(&replacement, &request).unwrap();
    assert!(host.begin_append("original-prepare-1", "new-context", &control).await.is_err());
    assert!(host.mirror.as_ref().unwrap().pending.as_ref().unwrap().batch.delivery.is_none());
    assert!(!host.directory.join("acknowledge.json").exists());
}

#[test]
fn same_caller_and_postintent_observations_cannot_offer_preintent_replacement() {
    let f = Fixture::new();
    let mut host = f.host(&caller());
    assert!(host.accept(&replacement_offer()).is_err());
    let mut host = f.host(&next_caller());
    let mut offered = replacement_offer();
    offered.pending.as_mut().unwrap().delivery = Some(intent(caller(), "old-context"));
    assert!(host.accept(&offered).is_err());
    assert!(!f.0.exists());
}

fn routed_observation() -> DeliveryObservation {
    let mut observed = observation();
    observed.acknowledged_cursor = cursor(Some(event(20, 0)));
    observed.routing_cursor = Some(event(9, 0));
    observed.routing_turn = true;
    let batch = observed.pending.as_mut().unwrap();
    let mut item = batch.page.items.remove(0);
    item.routing = Some(crate::collaboration::workflow::ResolutionRouting {
        requested_by: item.actor.clone(), linked_record: item.subject.clone(),
    });
    batch.page.from_cursor = observed.acknowledged_cursor.clone();
    batch.page.to_cursor = observed.acknowledged_cursor.clone();
    batch.page.through = event(21, 0);
    batch.page.has_more = true;
    batch.page.routing = Some(crate::collaboration::notifications::DeliveryRoutingPage {
        from_cursor: observed.routing_cursor.clone(), to_cursor: None,
        items: vec![item], has_more: false,
    });
    observed
}

fn routed_supersession(observed: &DeliveryObservation, original: &DeliveryIntent) -> DeliveryObservation {
    let batch = observed.pending.as_ref().unwrap();
    DeliveryObservation {
        acknowledged_cursor: observed.acknowledged_cursor.clone(), pending: None, available: None,
        routing_cursor: batch.page.routing.as_ref().unwrap().to_cursor.clone(), routing_turn: false,
        superseded: Some(DeliverySupersession {
            batch_id: batch.batch_id.clone(), operation_id: original.operation_id.clone(),
            caller: original.caller.clone(), context_binding: original.context_binding.clone(),
        }),
    }
}

#[test]
fn historical_delivery_fields_remain_absent_and_routed_older_event_keeps_exact_identity() {
    let f = Fixture::new();
    let mut host = f.host(&caller());
    let legacy = observation();
    let legacy_bytes = serde_json::to_vec(&legacy).unwrap();
    let legacy_json: serde_json::Value = serde_json::from_slice(&legacy_bytes).unwrap();
    for field in ["routing_cursor", "routing_turn", "superseded"] {
        assert!(legacy_json.get(field).is_none(), "historical field {field} must stay absent");
    }
    assert!(legacy_json["pending"]["page"].get("routing").is_none());
    assert!(legacy_json["pending"]["page"]["items"][0].get("routing").is_none());
    assert_eq!(serde_json::to_vec(&serde_json::from_slice::<DeliveryObservation>(&legacy_bytes).unwrap()).unwrap(), legacy_bytes);
    host.accept(&legacy).unwrap();
    let mirror: serde_json::Value = serde_json::from_slice(&std::fs::read(host.directory.join("delivery.json")).unwrap()).unwrap();
    for field in ["routing_cursor", "routing_turn", "superseded"] { assert!(mirror.get(field).is_none()); }
    drop(host);

    let f = Fixture::new();
    let host = f.host(&caller());
    let routed = routed_observation();
    host.validate_observation(&routed).unwrap();
    let batch = routed.pending.as_ref().unwrap();
    let route = batch.page.routing.as_ref().unwrap();
    assert!(route.items[0].id < routed.acknowledged_cursor.position.clone().unwrap());
    let value: serde_json::Value = serde_json::from_str(&host.encode(batch).unwrap()).unwrap();
    assert_eq!(value["events"], serde_json::to_value(&route.items).unwrap());
    assert_eq!(value["events"].as_array().unwrap().len(), 1);
    assert_eq!(value["continuation"], serde_json::Value::Null);
    assert_eq!(host.fitting_limit(&batch.page, &batch.batch_id).unwrap(), 1);
    for mode in ["duplicate", "mixed", "missing_intent", "wrong_scan_start"] {
        let mut changed = routed.clone();
        let page = &mut changed.pending.as_mut().unwrap().page;
        match mode {
            "duplicate" => { let item = page.routing.as_ref().unwrap().items[0].clone(); page.routing.as_mut().unwrap().items.push(item); }
            "mixed" => page.continuation = Some(continuation()),
            "missing_intent" => page.routing.as_mut().unwrap().items[0].routing = None,
            _ => page.routing.as_mut().unwrap().from_cursor = Some(event(8, 0)),
        }
        assert!(host.validate_observation(&changed).is_err(), "accepted {mode}");
    }
    assert!(!f.0.exists(), "validation neither appends nor ACKs an older routed event");
}

#[tokio::test]
async fn routed_begin_supersession_requires_original_committed_no_append_receipt() {
    let f = Fixture::new();
    let mut host = f.host(&caller());
    let observed = routed_observation();
    let original = intent(caller(), "route-context");
    let settled = routed_supersession(&observed, &original);
    host.accept(&observed).unwrap();
    host.current_confirmed = true;
    let original_bytes = std::fs::read(host.directory.join("delivery.json")).unwrap();
    assert!(host.accept(&settled).is_err(), "a current observation alone is not an original Begin receipt");
    assert_eq!(std::fs::read(host.directory.join("delivery.json")).unwrap(), original_bytes);
    retain_outcome(&host, "begin.json", Operation::BeginCollaborationDelivery {
        operation_id: original.operation_id.clone(), caller: caller(),
        batch_id: "original-prepare-1".into(), context_binding: original.context_binding.clone(),
    }, settled.clone());
    for mode in ["operation", "caller", "context", "routing_cursor"] {
        let mut changed = settled.clone();
        let proof = changed.superseded.as_mut().unwrap();
        match mode {
            "operation" => proof.operation_id = "different-begin".into(),
            "caller" => proof.caller = next_caller(),
            "context" => proof.context_binding = "different-context".into(),
            _ => changed.routing_cursor = Some(event(12, 0)),
        }
        assert!(host.accept(&changed).is_err(), "accepted {mode}");
        assert_eq!(std::fs::read(host.directory.join("delivery.json")).unwrap(), original_bytes);
    }
    let (_sender, control) = control();
    assert_eq!(host.begin_append("original-prepare-1", "route-context", &control).await.unwrap(), AppendDisposition::Superseded);
    assert_eq!(host.mirror.as_ref().unwrap().acknowledged_cursor, observed.acknowledged_cursor);
    assert!(host.mirror.as_ref().unwrap().pending.is_none());
    assert!(!host.directory.join("acknowledge.json").exists(), "no append was claimed, so there is no input ACK");
    assert!(host.read_mutation(&host.directory.join("begin.json")).unwrap().unwrap().consumed);
    drop(host);
    let reopened = f.host(&caller());
    assert!(!unresolved(&f.0, reopened.client.limits()).unwrap());
}

#[tokio::test]
async fn routed_no_append_receipt_survives_reopen_but_postintent_uncertainty_cannot_be_superseded() {
    let f = Fixture::new();
    let mut host = f.host(&caller());
    let observed = routed_observation();
    let original = intent(caller(), "route-context");
    let settled = routed_supersession(&observed, &original);
    host.accept(&observed).unwrap();
    retain_outcome(&host, "begin.json", Operation::BeginCollaborationDelivery {
        operation_id: original.operation_id.clone(), caller: caller(),
        batch_id: "original-prepare-1".into(), context_binding: original.context_binding.clone(),
    }, settled.clone());
    drop(host); // cut after the original receipt, before publishing the local mirror
    let mut reopened = f.host(&caller());
    let (_sender, control) = control();
    reopened.recover_route_supersession(&settled, &control).await.unwrap();
    reopened.accept(&settled).unwrap();
    drop(reopened); // cut after local mirror, before journal consumption
    let mut reopened = f.host(&caller());
    assert!(!unresolved(&f.0, reopened.client.limits()).unwrap());
    reopened.retire_begin(&control).await.unwrap();
    assert!(reopened.read_mutation(&reopened.directory.join("begin.json")).unwrap().unwrap().consumed);
    assert!(!reopened.directory.join("acknowledge.json").exists());

    let f = Fixture::new();
    let mut host = f.host(&caller());
    let begun = with_intent(observed, &original);
    host.accept(&begun).unwrap();
    host.mark_begin("original-prepare-1", &original).unwrap();
    let bytes = std::fs::read(host.directory.join("delivery.json")).unwrap();
    assert!(host.accept(&settled).is_err(), "claimed/unknown append cannot be reclassified as unclaimed");
    assert_eq!(std::fs::read(host.directory.join("delivery.json")).unwrap(), bytes);
    assert!(unresolved(&f.0, host.client.limits()).unwrap());
}

#[tokio::test]
async fn routed_ack_requires_exact_scan_end_even_when_ordinary_cursor_does_not_move() {
    for correct in [false, true] {
        let f = Fixture::new();
        let mut host = f.host(&caller());
        let original = intent(caller(), "route-context");
        let observed = with_intent(routed_observation(), &original);
        host.accept(&observed).unwrap();
        retain_outcome(&host, "begin.json", Operation::BeginCollaborationDelivery {
            operation_id: original.operation_id.clone(), caller: caller(),
            batch_id: "original-prepare-1".into(), context_binding: original.context_binding.clone(),
        }, observed.clone());
        host.mark_begin("original-prepare-1", &original).unwrap();
        let bytes = std::fs::read(host.directory.join("delivery.json")).unwrap();
        let acknowledged = DeliveryObservation {
            acknowledged_cursor: observed.acknowledged_cursor, pending: None, available: None,
            routing_cursor: if correct { None } else { Some(event(12, 0)) },
            routing_turn: false, superseded: None,
        };
        retain_outcome(&host, "acknowledge.json", Operation::AcknowledgeCollaborationDelivery {
            operation_id: "route-original-ack".into(), caller: caller(),
            batch_id: "original-prepare-1".into(), context_binding: original.context_binding.clone(),
        }, acknowledged);
        let (_sender, control) = control();
        let result = host.retire_ack(&control).await;
        if correct {
            result.unwrap();
            assert!(host.mirror.as_ref().unwrap().pending.is_none());
            assert!(!unresolved(&f.0, host.client.limits()).unwrap());
        } else {
            assert!(result.is_err());
            assert_eq!(std::fs::read(host.directory.join("delivery.json")).unwrap(), bytes);
            assert!(unresolved(&f.0, host.client.limits()).unwrap());
        }
    }
}


#[test]
fn routed_prefix_reserves_cursor_growth_before_prepare_and_persists_actual_endpoint() {
    fn opened(f: &Fixture, selected: WorkflowLimits, offered: &DeliveryBatch) -> Host {
        let mut host = f.with_policy(&caller(), selected);
        let state = DeliveryObservation {
            acknowledged_cursor: offered.page.from_cursor.clone(), pending: None,
            available: Some(offered.page.clone()),
            routing_cursor: offered.page.routing.as_ref().unwrap().from_cursor.clone(),
            routing_turn: true, superseded: None,
        };
        host.accept(&state).unwrap();
        host
    }
    fn mirror_bytes(host: &Host, batch: &DeliveryBatch) -> usize {
        let mut mirror = host.mirror.clone().unwrap();
        mirror.pending = Some(Pending {
            batch: batch.clone(), text: host.encode(batch).unwrap(), delivery: None,
        });
        serde_json::to_vec(&mirror).unwrap().len()
    }

    let f = Fixture::new();
    let mut offered = routed_observation().pending.unwrap();
    let route = offered.page.routing.as_mut().unwrap();
    let mut second = route.items[0].clone();
    second.id = event(20, 0);
    route.items.push(second);
    assert!(route.to_cursor.is_none());

    let mut old_prefix = offered.clone();
    old_prefix.page.routing.as_mut().unwrap().items.truncate(1);
    let mut actual_prefix = old_prefix.clone();
    let route = actual_prefix.page.routing.as_mut().unwrap();
    // The controller can scan unrelated rows after the retained item before
    // encountering the second owed event. Reserve more than the first item's ID.
    route.to_cursor = Some(event(19, (policy().events_per_mutation - 1) as u32));
    route.has_more = true;
    let mut selected = policy();
    selected.notice_bytes = serde_json::to_vec(&f.host(&caller()).encode(&actual_prefix).unwrap()).unwrap().len();
    assert!(f.with_policy(&caller(), selected.clone()).encode(&offered).is_err(),
        "the two-event notice must require a smaller Prepare limit");

    // Find the exact pre-correction mirror boundary, including the selected
    // policy's own serialized bytes. This is test fixture sizing, not a policy.
    for _ in 0..4 {
        let host = opened(&f, selected.clone(), &offered);
        let bytes = mirror_bytes(&host, &old_prefix);
        if bytes == selected.pending_delivery_bytes { break; }
        selected.pending_delivery_bytes = bytes;
    }
    let refused = opened(&f, selected.clone(), &offered);
    assert_eq!(mirror_bytes(&refused, &old_prefix), selected.pending_delivery_bytes);
    assert!(mirror_bytes(&refused, &actual_prefix) > selected.pending_delivery_bytes);
    assert!(refused.fitting_limit(&offered.page, &offered.batch_id).is_err(),
        "a None-end offer must not authorize an unpersistable Some-end Prepare");
    assert!(!f.0.exists(), "sizing cannot prepare or advance either cursor");

    // The reservation keeps false has_more's extra byte and the full u32
    // cursor width. The actual canonical endpoint uses a policy-bounded ordinal.
    let actual_ordinal = actual_prefix.page.routing.as_ref().unwrap().to_cursor.as_ref().unwrap().ordinal;
    let cursor_reserve_extra = u32::MAX.to_string().len() - actual_ordinal.to_string().len() + 1;
    for _ in 0..4 {
        let host = opened(&f, selected.clone(), &offered);
        let bytes = mirror_bytes(&host, &actual_prefix) + cursor_reserve_extra;
        if bytes == selected.pending_delivery_bytes { break; }
        selected.pending_delivery_bytes = bytes;
    }
    let mut fitted = opened(&f, selected.clone(), &offered);
    assert_eq!(mirror_bytes(&fitted, &actual_prefix) + cursor_reserve_extra, selected.pending_delivery_bytes);
    assert_eq!(fitted.fitting_limit(&offered.page, &offered.batch_id).unwrap(), 1);
    let prepared = DeliveryObservation {
        acknowledged_cursor: offered.page.from_cursor.clone(),
        routing_cursor: offered.page.routing.as_ref().unwrap().from_cursor.clone(),
        routing_turn: true, superseded: None, available: None,
        pending: Some(actual_prefix.clone()),
    };
    fitted.accept(&prepared).unwrap();
    let saved = fitted.mirror.as_ref().unwrap().pending.as_ref().unwrap();
    assert_eq!(saved.batch, actual_prefix);
    assert_eq!(saved.batch.page.routing.as_ref().unwrap().items,
        offered.page.routing.as_ref().unwrap().items[..1]);
    assert!(saved.delivery.is_none(), "local persistence is not an input ACK");
    drop(fitted);
    let reopened = f.with_policy(&caller(), selected);
    assert_eq!(reopened.mirror.as_ref().unwrap().pending.as_ref().unwrap().batch, actual_prefix);
}

#[test]
fn supersession_archive_retry_reestablishes_durability_before_mirror_replace() {
    let f = Fixture::new();
    let mut host = f.host(&next_caller());
    let offer = replacement_offer();
    host.accept(&offer).unwrap();
    let original = host.mirror.clone();
    let (_, replacement) = replacement_receipt();
    let batch = replacement.pending.as_ref().unwrap();
    let previous = original.as_ref().unwrap().pending.as_ref().unwrap();
    let mut directory = host.archive_directory("preintent", &previous.batch.batch_id);
    for chunk in component(&batch.batch_id).as_bytes().chunks(96) {
        directory.push(std::str::from_utf8(chunk).unwrap());
    }
    let path = directory.join("supersession.json");
    for _ in 0..2 {
        let cut = process::storage_tests::inject(
            &path, process::storage_tests::Point::DirectorySync, 5, 0,
        );
        assert!(host.accept(&replacement).is_err());
        assert_eq!(cut.hits(), 1);
        assert!(host.mirror == original, "mirror changed before archive durability");
        assert!(path.is_file());
        drop(cut);
    }
    let archived = std::fs::read(&path).unwrap();
    host.accept(&replacement).unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), archived);
    assert_eq!(host.mirror.as_ref().unwrap().pending.as_ref().unwrap().batch, *batch);
}

fn retained_input_ack(host: &mut Host) {
    let intent = intent(caller(), "thread-1-turn-2");
    let observed = with_intent(observation(), &intent);
    host.accept(&observed).unwrap();
    assert_eq!(host.mark_begin("original-prepare-1", &intent).unwrap(), AppendDisposition::Ready);
    let acknowledged = DeliveryObservation {
        acknowledged_cursor: observed.pending.unwrap().page.to_cursor,
        pending: None, available: None, routing_cursor: None, routing_turn: false, superseded: None,
    };
    retain_outcome(host, "acknowledge.json", Operation::AcknowledgeCollaborationDelivery {
        operation_id: "original-input-ack-1".into(), caller: caller(),
        batch_id: "original-prepare-1".into(), context_binding: intent.context_binding,
    }, acknowledged);
}

#[tokio::test]
async fn appended_receipt_returns_original_successful_ack_without_allocating_another_receipt() {
    let f = Fixture::new();
    let mut host = f.host(&caller());
    retained_input_ack(&mut host);
    let (_sender, control) = control();
    let id = host.appended_with_receipt("original-prepare-1", "thread-1-turn-2", &control).await.unwrap();
    assert_eq!(id, "original-input-ack-1");
    let path = host.directory.join("acknowledge.json");
    let recorded = host.read_mutation(&path).unwrap().unwrap();
    assert!(recorded.consumed);
    assert_eq!(recorded.outcome_context.unwrap().commit.operation_id, id);
    assert!(host.mirror.as_ref().unwrap().pending.is_none());
    let original = std::fs::read(&path).unwrap();
    assert_eq!(host.appended_with_receipt("original-prepare-1", "thread-1-turn-2", &control).await.unwrap(), id);
    assert_eq!(std::fs::read(&path).unwrap(), original);
}

#[tokio::test]
async fn appended_receipt_rejects_wrong_identity_refusal_uncommitted_or_unconsumed_journals() {
    let f = Fixture::new();
    let mut host = f.host(&caller());
    retained_input_ack(&mut host);
    let (_sender, control) = control();
    host.appended_with_receipt("original-prepare-1", "thread-1-turn-2", &control).await.unwrap();
    let path = host.directory.join("acknowledge.json");
    let original = host.read_mutation(&path).unwrap().unwrap();
    for mode in ["caller", "batch", "context", "refused", "uncommitted", "unconsumed", "pending", "commit-id"] {
        let mut changed = original.clone();
        match mode {
            "caller" | "batch" | "context" => {
                let Operation::AcknowledgeCollaborationDelivery { caller, batch_id, context_binding, .. } = &mut changed.request else { unreachable!() };
                match mode {
                    "caller" => caller.incarnation = "other-incarnation".into(),
                    "batch" => *batch_id = "other-batch".into(),
                    _ => *context_binding = "other-context".into(),
                }
            }
            "refused" => changed.outcome = Some(Outcome::Refused { code: "stale_revision".into(), message: "not acknowledged".into() }),
            "uncommitted" => changed.outcome_context = None,
            "unconsumed" => changed.consumed = false,
            "pending" => {
                let Some(Outcome::Ok { data: Data::CollaborationDelivery(observed) }) = &mut changed.outcome else { unreachable!() };
                observed.pending = observation().pending;
            }
            _ => changed.outcome_context.as_mut().unwrap().commit.operation_id = "different-operation".into(),
        }
        mutations::persist(&path, &changed).unwrap();
        assert!(host.appended_receipt("original-prepare-1", "thread-1-turn-2").is_err(), "accepted {mode}");
    }
    mutations::persist(&path, &original).unwrap();
    assert_eq!(host.appended_receipt("original-prepare-1", "thread-1-turn-2").unwrap(), "original-input-ack-1");
}

#[tokio::test]
async fn appended_receipt_reestablishes_ack_durability_after_reopen_and_repeated_sync_cuts() {
    let f = Fixture::new();
    let mut host = f.host(&caller());
    retained_input_ack(&mut host);
    let (_sender, control) = control();
    host.appended_with_receipt("original-prepare-1", "thread-1-turn-2", &control).await.unwrap();
    let path = host.directory.join("acknowledge.json");
    let original = std::fs::read(&path).unwrap();
    drop(host);
    for _ in 0..2 {
        let mut reopened = f.host(&caller());
        let cut = process::storage_tests::inject(&path, process::storage_tests::Point::DirectorySync, 5, 0);
        assert!(reopened.appended_with_receipt("original-prepare-1", "thread-1-turn-2", &control).await.is_err());
        assert_eq!(cut.hits(), 1);
        assert_eq!(std::fs::read(&path).unwrap(), original);
        assert!(reopened.mirror.as_ref().unwrap().pending.is_none());
        drop(cut);
    }
    let mut reopened = f.host(&caller());
    assert_eq!(reopened.appended_with_receipt("original-prepare-1", "thread-1-turn-2", &control).await.unwrap(), "original-input-ack-1");
    assert_eq!(std::fs::read(&path).unwrap(), original);
}

// Protocol fault fixture only; actual receiving-continuation/controller proof is
// exercised by native/completion_ack_recovery.rs. This peer permits one original
// mutation and at most two lookups, never a resend or an invented replacement ID.
struct AckLookupPeer {
    url: String,
    server: Option<tokio::task::JoinHandle<()>>,
    seen: Arc<std::sync::Mutex<Vec<Operation>>>,
}
impl AckLookupPeer {
    async fn start(original: Operation, outcome: Outcome, mode: &'static str,
        body_bytes: usize, stopped: Option<watch::Sender<bool>>, deadline: Option<u64>) -> Self {
        use bytes::Bytes;
        use http_body_util::{BodyExt, Full, Limited};
        use hyper::{body::Incoming, service::service_fn, Request as HttpRequest, Response};
        use hyper_util::rt::TokioIo;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let observed = seen.clone();
        let count = if matches!(mode, "post-contract" | "non-ack") { 2 } else { 3 };
        let server = tokio::spawn(async move {
            tokio::time::timeout(Duration::from_secs(10), async move {
                for index in 0..count {
                    let (stream, _) = listener.accept().await.unwrap();
                    let (original, outcome, seen, stopped) =
                        (original.clone(), outcome.clone(), observed.clone(), stopped.clone());
                    let service = service_fn(move |request: HttpRequest<Incoming>| {
                        let (original, outcome, seen, stopped) =
                            (original.clone(), outcome.clone(), seen.clone(), stopped.clone());
                        async move {
                            assert_eq!(request.method(), hyper::Method::POST);
                            assert_eq!(request.uri().path(), "/api/operations");
                            let body = Limited::new(request.into_body(), body_bytes).collect().await.unwrap().to_bytes();
                            let request: Request = serde_json::from_slice(&body).unwrap();
                            assert_eq!(request.protocol_revision, WIRE_REVISION);
                            let id = original.mutation_id().unwrap().to_owned();
                            let query = Operation::GetOperation { operation_id: id.clone() };
                            let expected = if index == 1 { &original } else { &query };
                            assert_eq!(serde_json::to_value(&request.command).unwrap(), serde_json::to_value(expected).unwrap());
                            seen.lock().unwrap().push(request.command.clone());
                            if index == 1 && mode != "post-contract" || index == 2 && mode == "unavailable" {
                                return Err(std::io::Error::other("controlled ACK transport loss"));
                            }
                            let mut reply = if index == 0 || index == 2 && mode == "absent" {
                                Reply::refused(request.request_id, "not_found", "no original receipt")
                            } else if index == 1 {
                                Reply { request_id: request.request_id, commit: Some(MutationCommit {
                                    operation_id: id.clone(), controller_revision: 17,
                                }), observed_controller_revision: Some(19), outcome: outcome.clone() }
                            } else {
                                let mut intended = original.clone();
                                if mode == "mismatch" {
                                    match &mut intended {
                                        Operation::AcknowledgeCollaborationDelivery { context_binding, .. } => context_binding.push_str("-other"),
                                        Operation::AcknowledgeCompletionInput(input) => input.begin_operation_id.push_str("-other"),
                                        _ => unreachable!(),
                                    }
                                }
                                Reply { request_id: request.request_id, commit: Some(MutationCommit {
                                    operation_id: id.clone(), controller_revision: 17,
                                }), observed_controller_revision: Some(19), outcome: Outcome::Ok {
                                    data: Data::Operation(OperationRecord {
                                        id, task_id: "reader-task".into(), attempt_id: "attempt-1".into(), generation: 1,
                                        kind: "application_mutation".into(), state: "settled".into(),
                                        intended: serde_json::to_value(intended).unwrap(),
                                        outcome: Some(serde_json::to_value(outcome).unwrap()),
                                    }),
                                } }
                            };
                            if index == 1 && mode == "post-contract" || index == 2 && mode == "lookup-contract" {
                                reply.request_id.push_str("-wrong");
                            }
                            if index == 2 {
                                if let Some(stopped) = stopped { stopped.send(true).unwrap(); }
                                if let Some(deadline) = deadline {
                                    tokio::time::sleep(Duration::from_millis(deadline.saturating_sub(process::now_ms()) + 1)).await;
                                }
                            }
                            let body = serde_json::to_vec(&reply).unwrap();
                            assert!(body.len() <= body_bytes);
                            Ok::<_, std::io::Error>(Response::builder().header("content-type", "application/json")
                                .header("connection", "close").body(Full::new(Bytes::from(body))).unwrap())
                        }
                    });
                    // Transport-loss and stopped-control cuts close the connection.
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service).await;
                }
            }).await.expect("bounded ACK lookup fixture timed out");
        });
        Self { url, server: Some(server), seen }
    }
    async fn finish(mut self, original: &Operation, count: usize) {
        self.server.take().unwrap().await.unwrap();
        let seen = self.seen.lock().unwrap();
        assert_eq!(seen.len(), count);
        assert_eq!(seen.iter().filter(|operation| operation.mutation_id() == original.mutation_id()).count(), 1,
            "recovery must not resend the ACK or allocate another mutation");
    }
}
impl Drop for AckLookupPeer {
    fn drop(&mut self) { if let Some(server) = self.server.take() { server.abort(); } }
}

//= spec/impl/v2/implementation.md#committed-resolutions-reach-their-consumers
//# A repeated resolution delivery MUST NOT start a second continuation for the same resolution occurrence.
#[tokio::test]
async fn lost_delivery_ack_uses_one_exact_lookup_and_retains_unknown_or_invalid_input() {
    for mode in ["recorded", "absent", "unavailable", "mismatch", "post-contract", "lookup-contract", "stopped", "deadline"] {
        let f = Fixture::new();
        let mut host = f.host(&caller());
        retained_input_ack(&mut host);
        let path = host.directory.join("acknowledge.json");
        let mut journal = host.read_mutation(&path).unwrap().unwrap();
        let outcome = journal.outcome.take().unwrap();
        journal.outcome_context = None;
        mutations::persist(&path, &journal).unwrap();
        let original = std::fs::read(&path).unwrap();
        let (sender, mut control) = control();
        if mode == "deadline" { control.deadline_ms = process::now_ms() + 1_000; }
        let peer = AckLookupPeer::start(journal.request.clone(), outcome, mode,
            host.client.limits().transport.body_bytes, (mode == "stopped").then_some(sender.clone()),
            (mode == "deadline").then_some(control.deadline_ms)).await;
        host.client = ApiClient::new(&peer.url, Arc::new(host.client.limits().clone())).unwrap();
        let result = host.appended_with_receipt("original-prepare-1", "thread-1-turn-2", &control).await;
        if mode == "recorded" {
            assert_eq!(result.unwrap(), "original-input-ack-1");
            let retained = host.read_mutation(&path).unwrap().unwrap();
            assert!(retained.consumed);
            assert_eq!(retained.outcome_context.unwrap().commit.controller_revision, 17);
            assert!(host.mirror.as_ref().unwrap().pending.is_none());
        } else {
            assert!(result.is_err(), "unexpected recovery for {mode}");
            assert_eq!(std::fs::read(&path).unwrap(), original, "unknown original ACK must remain exact: {mode}");
            let pending = host.mirror.as_ref().unwrap().pending.as_ref().unwrap();
            assert!(pending.delivery.as_ref().unwrap().appended, "actual append evidence must remain pending");
        }
        peer.finish(&journal.request, if mode == "post-contract" { 2 } else { 3 }).await;
    }
}

#[tokio::test]
async fn lost_completion_ack_uses_one_exact_lookup_without_widening_other_mutations() {
    use crate::governance_completion::{AcknowledgeCompletionInput, CompletionBoundary, CompletionInputAck, CompletionOutcome, CompletionTarget};
    for mode in ["recorded", "absent", "unavailable", "mismatch", "post-contract", "lookup-contract", "non-ack"] {
        let f = Fixture::new();
        process::private_directory(&f.0).unwrap();
        let limits = crate::worker::completion::tests::policy();
        let mut value = crate::worker::completion::tests::feedback(
            CompletionBoundary::NativeResponse { request_index: 0 }, CompletionTarget::NativeRequest { request_index: 1 }).continuation;
        let ack = AcknowledgeCompletionInput {
            operation_id: "original-completion-ack".into(), caller: value.caller.clone(),
            begin_operation_id: value.begin_operation_id.clone(), consumed_at: CompletionBoundary::NativeResponse { request_index: 1 },
            delivery_ack_operation_id: None, native_input: Some(crate::governance_completion::NativeCompletionInput {
                request_binding: crate::native::ProviderRequestBinding { bytes: 128, sha256: "ab".repeat(32) }, rejections: vec![],
            }),
        };
        value.acknowledged = Some(CompletionInputAck { operation_id: ack.operation_id.clone(), consumed_at: ack.consumed_at.clone(),
            delivery_ack_operation_id: ack.delivery_ack_operation_id.clone(), native_input: ack.native_input.clone() });
        let outcome = Outcome::Ok { data: Data::Completion(Box::new(CompletionOutcome::InputAcknowledged(value))) };
        let request = if mode == "non-ack" {
            Operation::RegisterWorker { operation_id: "original-registration".into(), worker: Worker {
                id: "worker".into(), incarnation: "one".into(), coding_capacity: 0, native_capacity: 1,
            } }
        } else { Operation::AcknowledgeCompletionInput(ack) };
        let path = f.0.join("acknowledge.json");
        let mut journal = mutations::create(&path, request.clone(), 0, &limits).unwrap();
        let original = std::fs::read(&path).unwrap();
        let peer = AckLookupPeer::start(request.clone(), outcome.clone(), mode, limits.transport.body_bytes, None, None).await;
        let client = ApiClient::new(&peer.url, Arc::new(limits)).unwrap();
        let result = mutations::resolve(&client, &path, &mut journal).await;
        if mode == "recorded" {
            assert_eq!(result.unwrap(), Some(outcome));
            let retained = mutations::read(&path, client.limits()).unwrap().unwrap();
            assert!(!retained.consumed, "only the completion Host can validate actual input and consume its ACK");
            assert_eq!(retained.outcome_context.unwrap().commit.controller_revision, 17);
        } else {
            assert!(!matches!(result, Ok(Some(_))), "unexpected recovery for {mode}");
            assert_eq!(std::fs::read(&path).unwrap(), original);
        }
        peer.finish(&request, if matches!(mode, "post-contract" | "non-ack") { 2 } else { 3 }).await;
    }
}
