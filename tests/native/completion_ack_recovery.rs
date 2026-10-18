//! Actual native completion input; both committed ACK replies are lost once.
//! The worker must recover each original receipt itself before returning seven.
use super::*;
use borg_runtime::collaboration::{
    documents::{DocumentContent, DocumentMutation, DocumentMutationOutcome},
    notifications::{DeliveryBatch, DeliveryObservation},
    questions::{Question, QuestionAnswer},
    work::WorkMutation,
    workflow::{ActivityDetail, ActorProvenance, QuestionChange, ResolverRef, WorkflowActivity},
};

#[derive(Clone)]
struct Exchange { request: Request, reply: Reply, dropped: bool }
struct CutState {
    event: Option<EventId>, batch: Option<String>, delivery_dropped: bool,
    completion_dropped: bool, exchanges: Vec<Exchange>,
}
struct AckProxy { url: String, state: Arc<Mutex<CutState>>, _owner: Background }
impl AckProxy {
    async fn start(upstream: String, task_id: String, bound: usize) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let state = Arc::new(Mutex::new(CutState { event: None, batch: None,
            delivery_dropped: false, completion_dropped: false, exchanges: vec![] }));
        let saved = state.clone();
        let owner = tokio::spawn(async move {
            let client = reqwest::Client::builder().no_proxy().build().unwrap();
            let mut connections = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (stream, _) = accepted.unwrap();
                        let (upstream, task_id, saved, client) = (upstream.clone(), task_id.clone(), saved.clone(), client.clone());
                        connections.spawn(async move {
                            let service = service_fn(move |request: HttpRequest<Incoming>| {
                                let (upstream, task_id, saved, client) = (upstream.clone(), task_id.clone(), saved.clone(), client.clone());
                                async move {
                                    let path = request.uri().path().to_owned();
                                    assert!(matches!(path.as_str(), "/mcp" | "/api/operations"));
                                    let caller = request.headers().get("x-borg-caller").cloned();
                                    let version = request.headers().get("mcp-protocol-version").cloned();
                                    let bytes = request.into_body().collect().await.unwrap().to_bytes();
                                    assert!(bytes.len() <= bound);
                                    let operation: Option<Request> = (path == "/api/operations")
                                        .then(|| serde_json::from_slice(&bytes).unwrap());
                                    let mut forward = client.post(format!("{upstream}{path}"))
                                        .header("content-type", "application/json").body(bytes.to_vec());
                                    if let Some(value) = caller { forward = forward.header("x-borg-caller", value); }
                                    if let Some(value) = version { forward = forward.header("mcp-protocol-version", value); }
                                    let response = forward.send().await.map_err(io::Error::other)?;
                                    let status = response.status();
                                    let bytes = response.bytes().await.map_err(io::Error::other)?;
                                    assert!(bytes.len() <= bound);
                                    let mut dropped = false;
                                    if let Some(request) = operation {
                                        let selected = matches!(&request.command,
                                            Operation::GetOperation { .. }
                                            | Operation::PrepareCollaborationDelivery { .. }
                                            | Operation::BeginCollaborationDelivery { .. }
                                            | Operation::AcknowledgeCollaborationDelivery { .. }
                                            | Operation::EvaluateCompletion(_)
                                            | Operation::BeginCompletionContinuation(_)
                                            | Operation::AcknowledgeCompletionInput(_));
                                        if selected {
                                            assert!(status.is_success());
                                            let reply: Reply = serde_json::from_slice(&bytes).unwrap();
                                            assert_eq!(reply.request_id, request.request_id);
                                            reply.validate_for(&request.command).unwrap();
                                            let mut state = saved.lock().unwrap();
                                            if let Operation::PrepareCollaborationDelivery { caller, .. } = &request.command {
                                                if caller.task_id == task_id {
                                                    if let Outcome::Ok { data: Data::CollaborationDelivery(view) } = &reply.outcome {
                                                        if let Some(batch) = view.pending.as_ref().filter(|batch|
                                                            state.event.as_ref().is_some_and(|event| batch.page.contains_event(event))) {
                                                            assert!(state.batch.is_none(), "a second required Prepare was sent");
                                                            state.batch = Some(batch.batch_id.clone());
                                                        }
                                                    }
                                                }
                                            }
                                            let cut_delivery = matches!(&request.command,
                                                Operation::AcknowledgeCollaborationDelivery { caller, batch_id, .. }
                                                if caller.task_id == task_id && state.batch.as_ref() == Some(batch_id));
                                            let cut_completion = matches!(&request.command,
                                                Operation::AcknowledgeCompletionInput(input) if input.caller.task_id == task_id);
                                            if cut_delivery || cut_completion {
                                                assert!(matches!(reply.outcome, Outcome::Ok { .. }));
                                                assert!(reply.commit.is_some(), "cut only after canonical commit");
                                                if cut_delivery {
                                                    assert!(!state.delivery_dropped, "delivery ACK was resent");
                                                    state.delivery_dropped = true;
                                                } else {
                                                    assert!(state.delivery_dropped);
                                                    assert!(!state.completion_dropped, "completion ACK was resent");
                                                    state.completion_dropped = true;
                                                }
                                                dropped = true;
                                            }
                                            assert!(state.exchanges.len() < 128, "bounded original-operation transcript");
                                            state.exchanges.push(Exchange { request, reply, dropped });
                                        }
                                    }
                                    if dropped {
                                        return Err(io::Error::new(io::ErrorKind::ConnectionAborted,
                                            "controlled loss of committed ACK reply"));
                                    }
                                    Ok::<_, io::Error>(Response::builder().status(status.as_u16()).body(Full::new(bytes)).unwrap())
                                }
                            });
                            let _ = hyper::server::conn::http1::Builder::new()
                                .serve_connection(TokioIo::new(stream), service).await;
                        });
                    }
                    _ = connections.join_next(), if !connections.is_empty() => {}
                }
            }
        });
        Self { url, state, _owner: Background(owner) }
    }
}

struct Gate { entered: tokio::sync::Notify, release: tokio::sync::Notify }
impl Gate {
    fn new() -> Arc<Self> { Arc::new(Self { entered: tokio::sync::Notify::new(), release: tokio::sync::Notify::new() }) }
    async fn hold(&self) {
        self.entered.notify_one();
        tokio::time::timeout(Duration::from_secs(30), self.release.notified()).await.unwrap();
    }
    async fn wait(&self) { tokio::time::timeout(Duration::from_secs(30), self.entered.notified()).await.unwrap(); }
}
async fn ok(api: &ApiClient, operation: Operation) -> Reply {
    let reply = api.call(operation.clone()).await.unwrap();
    reply.validate_for(&operation).unwrap(); assert!(matches!(reply.outcome, Outcome::Ok { .. }), "{reply:?}"); reply
}
fn native_input(lab: &Lab, agent: &str, requests: u32) -> TaskInput {
    let mut value = input(lab, requests);
    value.definition.id = agent.into(); value.definition.revision = "completion-ack-v2".into();
    value.definition.execution_profile = Some(ExecutionProfile::CollaborationV2 { publication: None, web_search: None }); value
}
fn charter_tool(mut reply: Value, declaration: &Value) -> Value {
    reply["output"].as_array_mut().unwrap().insert(0, json!({"id":"asker-charter", "type":"message", "role":"assistant", "status":"completed", "phase":"commentary",
        "content":[{"type":"output_text", "text":json!({"charter_references":declaration}).to_string()}]})); reply
}
fn charter_final(declaration: &Value) -> Value {
    let mut reply = final_value();
    reply["output"][0]["content"][0]["text"] = json!({"charter_references":declaration, "result":7}).to_string().into(); reply
}
fn sha(bytes: &[u8]) -> String {
    aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, bytes).as_ref().iter().map(|b| format!("{b:02x}")).collect()
}
fn read(path: &Path) -> Value { serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap() }
fn notice_dir(lab: &Lab, task: &Task) -> PathBuf {
    let mut path = lab.state().join("collaboration-notices");
    for (tag, value) in [("task", &task.id), ("session", &task.session_id), ("agent", &task.assigned_agent_id)] {
        path.push(tag);
        for part in process::component(value).as_bytes().chunks(96) { path.push(std::str::from_utf8(part).unwrap()); }
    }
    path
}
async fn event(api: &ApiClient, id: &EventId) -> WorkflowActivity {
    let mut cursor = None;
    for _ in 0..8 {
        let reply = ok(api, Operation::QueryWorkflowActivity { cursor, limit: 8 }).await;
        let Outcome::Ok { data: Data::WorkflowActivity(page) } = reply.outcome else { unreachable!() };
        if let Some(event) = page.items.into_iter().find(|event| event.id == *id) { return event; }
        cursor = page.next_cursor; if cursor.is_none() { break; }
    }
    panic!("original terminal event absent")
}
fn delivery(reply: Reply) -> DeliveryObservation {
    let Outcome::Ok { data: Data::CollaborationDelivery(value) } = reply.outcome else { panic!("{reply:?}") }; value
}
fn recovered(exchanges: &[Exchange], original: &Exchange) -> Exchange {
    let id = original.reply.commit.as_ref().unwrap().operation_id.clone();
    let cut = exchanges.iter().position(|row| row.dropped && row.reply.commit == original.reply.commit).unwrap();
    let after: Vec<_> = exchanges[cut + 1..].iter().filter(|row|
        matches!(&row.request.command, Operation::GetOperation { operation_id } if operation_id == &id)).collect();
    assert_eq!(after.len(), 1, "one original lookup after the lost reply");
    let lookup = after[0];
    let Outcome::Ok { data: Data::Operation(record) } = &lookup.reply.outcome else { panic!("{:#?}", lookup.reply) };
    assert_eq!(record.id, id); assert_eq!(record.kind, "application_mutation"); assert_eq!(record.state, "settled");
    assert_eq!(record.intended, json!(original.request.command));
    assert_eq!(record.outcome, Some(json!(original.reply.outcome)));
    assert_eq!(lookup.reply.commit, original.reply.commit);
    assert!(lookup.reply.observed_controller_revision >= original.reply.observed_controller_revision);
    lookup.clone()
}
fn journal_matches(path: &Path, original: &Exchange, lookup: &Exchange) -> Vec<u8> {
    let bytes = std::fs::read(path).unwrap(); let value: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(value["request"], json!(original.request.command));
    assert_eq!(value["outcome"], json!(original.reply.outcome)); assert_eq!(value["consumed"], true);
    assert_eq!(value["outcome_context"]["commit"], json!(original.reply.commit.as_ref().unwrap()));
    assert_eq!(value["outcome_context"]["observed_controller_revision"], json!(lookup.reply.observed_controller_revision)); bytes
}
fn inventory(lab: &Lab, task: &Task, owner: &Task) -> Value {
    let db = rusqlite::Connection::open_with_flags(lab.root.join("tasks.sqlite"), rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let count = |table: &str| db.query_row(&format!("SELECT count(*) FROM {table}"), [], |row| row.get::<_, i64>(0)).unwrap();
    assert_eq!(count("tasks"), 3, "one real governance bootstrap, asker and owner");
    assert_eq!(count("sessions"), 3); assert_eq!(count("attempts"), 2, "bootstrap and one actual asker attempt");
    assert_eq!(db.query_row("SELECT count(*) FROM attempts WHERE task_id=?1", [&task.id], |row| row.get::<_, i64>(0)).unwrap(), 1);
    assert_eq!(db.query_row("SELECT count(*) FROM attempts WHERE task_id=?1", [&owner.id], |row| row.get::<_, i64>(0)).unwrap(), 0);
    let mut rows = db.prepare("SELECT body FROM commands ORDER BY seq LIMIT 32").unwrap();
    let commands: Vec<String> = rows.query_map([], |row| row.get(0)).unwrap().map(Result::unwrap).collect();
    assert!(commands.len() < 32);
    let mut runs = vec![];
    for raw in &commands {
        if let WorkerCommand::RunAttempt { task: input, attempt, session, .. } = serde_json::from_str(raw).unwrap() {
            if attempt.task_id == task.id {
                assert_eq!(input, task.input); assert_eq!(attempt.session_id, task.session_id);
                assert_eq!(session.id, task.session_id); assert_eq!(attempt.id, task.attempt.as_ref().unwrap().id);
            }
            assert_ne!(attempt.task_id, owner.id); runs.push(attempt.task_id);
        }
    }
    assert_eq!(runs.len(), 2); assert_eq!(runs.iter().filter(|id| *id == &task.id).count(), 1);
    json!({"tasks":count("tasks"),"sessions":count("sessions"),"attempts":count("attempts"),"commands":commands})
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]

// Traceability scope: The actual native continuation retains the exact resolution and remaining-work input before both ACKs; lost replies reconcile without reappend.
//= spec/impl/v2/implementation.md#committed-resolutions-reach-their-consumers
//= type=test
//# A resolution notice MUST be delivered to an active executor at a supported safe input boundary.
//= spec/impl/v2/implementation.md#committed-resolutions-reach-their-consumers
//= type=test
//# A resolution-delivery acknowledgment MUST follow durable retention of the outcome for its receiving continuation.
//= spec/impl/v2/implementation.md#voluntary-completion-checks-remaining-work
//= type=test
//# A permitted continuation after refused voluntary completion MUST receive the recorded remaining-work reason as input.

async fn active_asker_completion_recovers_both_committed_acks_without_reappend() {
    Box::pin(scenario()).await;
}
async fn scenario() {
    let mut lab = Lab::new("native-completion-ack-loss"); governed(&mut lab);
    let mut controller = lab.controller("controller").await;
    let api = ApiClient::new(&lab.url, lab.limits.clone()).unwrap(); promote(&api, &lab).await;
    let task = submit_identified(&api, native_input(&lab, "active-asker", 3), "submit-active-asker").await;
    let owner = submit_identified(&api, native_input(&lab, "document-owner", 1), "submit-document-owner").await;
    let accepted_charter = binding(&api, &task.session_id).await;
    let accepted_ref = CharterSourceRef { definition_id: task.input.definition.id.clone(), definition_revision: task.input.definition.revision.clone() };
    let DefinitionCharter::Inline { source: accepted_source } = task.input.definition.charter.clone().unwrap() else { panic!("exact accepted inline source") };
    assert_eq!(accepted_charter.session.effective, accepted_ref);
    assert_eq!(accepted_charter.charter, ResolvedCharter { reference: accepted_ref.clone(), source: accepted_source.clone() });
    assert!(accepted_charter.session.pending.is_none() && accepted_charter.session.compaction_due.is_none() && accepted_charter.session.compaction_in_flight.is_none());
    let declaration = json!(DeclaredCharterReferences { revision: accepted_ref, clauses: accepted_source.clauses.clone() });
    // The distinct responsible owner remains eligible for routing but has an
    // explicit independent wait, so success cannot dispatch an unrelated POST.
    let paused = ok(&api, Operation::MutateQuestion { operation_id: "owner-pause-question".into(),
        mutation: QuestionMutation::Pose { question: question(&owner.id) } }).await;
    let Outcome::Ok { data: Data::QuestionMutation(paused) } = paused.outcome else { unreachable!() };
    ok(&api, Operation::MutateWork { operation_id: "owner-explicit-wait".into(), mutation: WorkMutation::AddWait {
        task_id: owner.id.clone(), expected_revision: 0,
        resolver: ResolverRef::Question { question_id: paused.question.question.id },
    }}).await;
    let owner = get(&api, &owner.id).await; assert_eq!(owner.status, TaskStatus::Queued); assert!(owner.attempt.is_none());
    let created = ok(&api, Operation::MutateDocument { operation_id: "create-owned-document".into(),
        mutation: DocumentMutation::Create { content: DocumentContent { title: "Active asker priority".into(), body: "Keep seven".into() },
            supplied_content_id: None, responsible_owner: PrincipalRef::Agent { agent_id: owner.assigned_agent_id.clone() } },
    }).await;
    let Outcome::Ok { data: Data::DocumentMutation(DocumentMutationOutcome::Document { view }) } = created.outcome else { unreachable!() };
    let mut pose = question(&task.id); pose.record = RecordRef::Document { document_id: view.document.id.clone() };
    let final_gate = Gate::new(); let input_gate = Gate::new();
    let (final_hold, input_hold) = (final_gate.clone(), input_gate.clone());
    let posed = Arc::new(Mutex::new(None::<Question>)); let posed_peer = posed.clone();
    let mut peer = BoundedPeer::start(api.clone(), task.id.clone(), lab.limits.clone(), 3, move |index, request| {
        let (final_hold, input_hold, posed, pose, declaration) = (final_hold.clone(), input_hold.clone(), posed_peer.clone(), pose.clone(), declaration.clone());
        async move { match index {
            0 => HttpReply::ok(charter_tool(function(0, "collab_question_pose", json!({"question":pose})), &declaration)),
            1 => {
                let outputs: Vec<_> = request["input"].as_array().unwrap().iter().filter(|item| item["type"] == "function_call_output").collect();
                assert_eq!(outputs.len(), 1); assert_eq!(outputs[0]["call_id"], "call-0");
                let output: Value = serde_json::from_str(outputs[0]["output"].as_str().unwrap()).unwrap();
                let Outcome::Ok { data: Data::QuestionMutation(value) } = serde_json::from_value(output["outcome"].clone()).unwrap() else { panic!("actual poser receipt") };
                *posed.lock().unwrap() = Some(value.question.question);
                final_hold.hold().await; HttpReply::ok(charter_final(&declaration))
            }
            2 => { input_hold.hold().await; HttpReply::ok(charter_final(&declaration)) }
            _ => panic!("extra continuation or replayed input"),
        }}
    }).await;
    let proxy = AckProxy::start(lab.url.clone(), task.id.clone(), lab.limits.transport.body_bytes).await;
    let mut worker = lab.worker(&proxy.url, &peer.endpoint, "completion-ack-worker");
    final_gate.wait().await;
    let active = get(&api, &task.id).await; assert_eq!(active.status, TaskStatus::Active);
    let attempt = active.attempt.clone().unwrap(); let actor = caller(&attempt);
    assert_eq!(attempt.phase, AttemptPhase::Running); assert!(!attempt.stopped && !attempt.uncertain);
    assert_eq!(active.input, task.input); assert_eq!(active.session_id, task.session_id);
    let question = posed.lock().unwrap().clone().unwrap();
    assert_eq!(question.poser.principal, PrincipalRef::Agent { agent_id: task.assigned_agent_id.clone() });
    assert_eq!(question.poser.provenance, ActorProvenance::BoundAgentSession {
        agent_id: task.assigned_agent_id.clone(), session_id: task.session_id.clone(), task_id: task.id.clone(),
        attempt_id: attempt.id.clone(), generation: attempt.generation,
        worker_id: attempt.worker_id.clone(), incarnation: attempt.worker_incarnation.clone(),
    });
    let answered = ok(&api, Operation::MutateQuestion { operation_id: "answer-during-final-response".into(),
        mutation: QuestionMutation::Answer { question_id: question.id.clone(), expected_revision: question.revision,
            answer: QuestionAnswer { option_id: "keep".into(), explanation: "Finish seven after observing this exact resolution".into() } },
    }).await;
    assert!(answered.commit.is_some());
    let terminal = event(&api, &closed_event(&api, &question.id).await).await;
    assert!(matches!(terminal.detail, ActivityDetail::Question(QuestionChange::Closed { .. })));
    assert_eq!(terminal.routing.as_ref().unwrap().requested_by, question.poser);
    assert_eq!(terminal.routing.as_ref().unwrap().linked_record, question.posed.record);
    proxy.state.lock().unwrap().event = Some(terminal.id.clone());
    final_gate.release.notify_one(); input_gate.wait().await;
    assert_eq!(peer.count(), 3);
    let running = get(&api, &task.id).await; assert_eq!(running.input, task.input);
    assert_eq!(running.attempt.as_ref().unwrap().id, attempt.id); assert_eq!(running.status, TaskStatus::Active);
    assert_eq!(get(&api, &owner.id).await, owner, "current document owner did not supersede the active asker");
    let observed = delivery(ok(&api, Operation::ObserveCollaborationDelivery { caller: actor.clone(), limit: 8 }).await);
    let batch: DeliveryBatch = observed.pending.unwrap(); assert!(batch.page.contains_event(&terminal.id));
    assert_eq!(batch.page.from_cursor.scope.task_id, task.id);
    assert_eq!(batch.page.from_cursor.scope.session_id, task.session_id);
    assert_eq!(batch.delivery.as_ref().unwrap().caller, actor);
    let mirror_path = notice_dir(&lab, &task).join("delivery.json"); let before_mirror = read(&mirror_path);
    assert_eq!(before_mirror["pending"]["batch"], json!(batch));
    assert_eq!(before_mirror["pending"]["delivery"]["appended"], false);
    let request: Value = serde_json::from_slice(&peer.bodies.lock().unwrap()[2]).unwrap();
    let developer: Vec<_> = request["input"].as_array().unwrap().iter().filter(|item| item["role"] == "developer").collect();
    assert_eq!(developer.iter().filter(|item| item["content"] == before_mirror["pending"]["text"]).count(), 1);
    let feedback = developer.iter().find_map(|item| item["content"].as_str()?.strip_prefix(
        "The controller refused voluntary completion. Address this recorded remaining work within the existing task authority and limits:\n"))
        .map(|text| serde_json::from_str::<Value>(text).unwrap()).expect("actual required completion feedback");
    assert_eq!(serde_json::from_value::<Vec<CompletionReason>>(feedback["remaining_work"].clone()).unwrap(),
        vec![CompletionReason::UnconsumedInput { event_id: terminal.id.clone() }]);
    let boundary = CompletionBoundary::NativeResponse { request_index: 2 };
    let decision = DecisionRef { operation_id: evaluation_id(&actor, &boundary).unwrap(), point: DecisionPoint::Completion };
    let original_decision = query(&api, GovernanceQuery::Decision { decision: decision.clone() }).await;
    let GovernanceResult::Decision(ref row) = original_decision else { panic!("original enforcing decision") };
    assert_eq!(row.mode, DecisionMode::Enforcing);
    let DecisionDetail::Completion(ref state) = row.detail else { unreachable!() };
    assert_eq!(state.consecutive_count, 1);
    assert_eq!(state.disposition, CompletionDisposition::Blocked { continuation_eligible: true, exhausted: false });
    let completion_dir = lab.state().join("completion").join(sha(&serde_json::to_vec(&actor).unwrap())).join(sha(&serde_json::to_vec(&boundary).unwrap()));
    let before_input = read(&completion_dir.join("input.json"));
    assert_eq!(before_input["started"], true); assert!(before_input["consumed_at"].is_null());
    assert!(!completion_dir.join("acknowledge.json").exists());
    { let state = proxy.state.lock().unwrap(); assert!(!state.delivery_dropped && !state.completion_dropped); }
    input_gate.release.notify_one();
    let done = until(&api, &task.id, |task| task.status == TaskStatus::Succeeded && task.attempt.as_ref().is_some_and(|attempt| attempt.stopped)).await;
    assert_eq!(done.input, task.input); assert_eq!(done.session_id, task.session_id); assert_eq!(done.assigned_agent_id, task.assigned_agent_id);
    let final_attempt = done.attempt.as_ref().unwrap(); assert_eq!(final_attempt.id, attempt.id);
    assert_eq!(final_attempt.generation, attempt.generation); assert_eq!(final_attempt.deadline_unix_ms, attempt.deadline_unix_ms);
    assert!(!final_attempt.uncertain); assert_eq!(done.result.as_ref().unwrap().output, Some(json!(7)));
    assert_eq!(peer.count(), 3); assert_eq!(live_usage(&api, final_attempt).await, (3, Some(15), Some(6)));
    assert_eq!(query(&api, GovernanceQuery::Decision { decision: decision.clone() }).await, original_decision);
    let final_decision_ref = DecisionRef { operation_id: evaluation_id(&actor,
        &CompletionBoundary::NativeResponse { request_index: 3 }).unwrap(), point: DecisionPoint::Completion };
    let final_decision = query(&api, GovernanceQuery::Decision { decision: final_decision_ref.clone() }).await;
    let GovernanceResult::Decision(ref final_row) = final_decision else { panic!("final enforcing decision") };
    assert_eq!(final_row.mode, DecisionMode::Enforcing);
    let DecisionDetail::Completion(ref final_state) = final_row.detail else { unreachable!() };
    assert_eq!(final_state.disposition, CompletionDisposition::Admit);
    assert_eq!(get(&api, &owner.id).await, owner);
    let final_charter = binding(&api, &task.session_id).await;
    assert_eq!(final_charter.charter, accepted_charter.charter);
    assert!(final_charter.session.pending.is_none() && final_charter.session.compaction_due.is_none() && final_charter.session.compaction_in_flight.is_none());
    worker.stop().await; controller.stop().await; peer.close().await;
    let exchanges = proxy.state.lock().unwrap().exchanges.clone();
    let lost: Vec<_> = exchanges.iter().filter(|row| row.dropped).cloned().collect(); assert_eq!(lost.len(), 2);
    let delivery_ack = &lost[0]; let completion_ack = &lost[1];
    let recovered_delivery = recovered(&exchanges, delivery_ack); let recovered_completion = recovered(&exchanges, completion_ack);
    let Operation::AcknowledgeCollaborationDelivery { caller: delivered_actor, batch_id, context_binding, operation_id: delivery_id } = &delivery_ack.request.command else { panic!("first original ACK") };
    assert_eq!(delivered_actor, &actor); assert_eq!(batch_id, &batch.batch_id); assert_eq!(context_binding, &attempt.id);
    let Operation::AcknowledgeCompletionInput(ack) = &completion_ack.request.command else { panic!("second original ACK") };
    assert_eq!(ack.caller, actor); assert_eq!(ack.delivery_ack_operation_id.as_ref(), Some(delivery_id));
    assert_eq!(ack.consumed_at, CompletionBoundary::NativeResponse { request_index: 3 });
    assert!(ack.native_input.as_ref().unwrap().rejections.is_empty());
    let actual_request = peer.bodies.lock().unwrap()[2].clone();
    assert_eq!(ack.native_input.as_ref().unwrap().request_binding,
        borg_runtime::native::ProviderRequestBinding { bytes: actual_request.len() as u64, sha256: sha(&actual_request) });
    assert_eq!(ack.begin_operation_id, begin_id(&actor, &decision).unwrap());
    let begins: Vec<_> = exchanges.iter().filter_map(|row| match &row.request.command { Operation::BeginCompletionContinuation(value) => Some(value), _ => None }).collect();
    assert_eq!(begins.len(), 1); assert_eq!(begins[0].target, CompletionTarget::NativeRequest { request_index: 3 });
    assert_eq!(begins[0].delivery_batch_id.as_ref(), Some(&batch.batch_id));
    let notice_ack_path = notice_dir(&lab, &task).join("acknowledge.json");
    let notice_ack_bytes = journal_matches(&notice_ack_path, delivery_ack, &recovered_delivery);
    let completion_ack_path = completion_dir.join("acknowledge.json");
    let completion_ack_bytes = journal_matches(&completion_ack_path, completion_ack, &recovered_completion);
    let input_bytes = std::fs::read(completion_dir.join("input.json")).unwrap();
    let input: Value = serde_json::from_slice(&input_bytes).unwrap();
    assert_eq!(input["feedback"], before_input["feedback"]); assert_eq!(input["text"], before_input["text"]);
    assert_eq!(input["consumed_at"], json!(ack.consumed_at)); assert_eq!(input["native_input"], json!(ack.native_input));
    assert!(read(&mirror_path)["pending"].is_null());
    let mirror_bytes = std::fs::read(&mirror_path).unwrap();
    let db = rusqlite::Connection::open_with_flags(lab.root.join("tasks.sqlite"), rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    for original in &lost {
        let commit = original.reply.commit.as_ref().unwrap();
        let (request, outcome, revision): (String, String, i64) = db.query_row("SELECT request,outcome,controller_revision FROM receipts WHERE id=?1", [&commit.operation_id], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))).unwrap();
        assert_eq!(serde_json::from_str::<Value>(&request).unwrap(), json!(original.request.command));
        assert_eq!(serde_json::from_str::<Outcome>(&outcome).unwrap(), original.reply.outcome); assert_eq!(u64::try_from(revision).expect("nonnegative original revision"), commit.controller_revision);
    }
    assert_eq!(db.query_row("SELECT count(*) FROM attempts WHERE task_id=?1", [&task.id], |row| row.get::<_, i64>(0)).unwrap(), 1);
    drop(db);
    let original_inventory = inventory(&lab, &done, &owner);
    let mut reopened = lab.controller("controller-reopened").await;
    assert_eq!(get(&api, &task.id).await, done); assert_eq!(get(&api, &owner.id).await, owner);
    for original in &lost {
        let row = receipt(&api, &original.reply.commit.as_ref().unwrap().operation_id).await.unwrap();
        assert_eq!(row.intended, json!(original.request.command)); assert_eq!(row.outcome, Some(json!(original.reply.outcome)));
    }
    assert_eq!(event(&api, &terminal.id).await, terminal);
    assert_eq!(query(&api, GovernanceQuery::Decision { decision }).await, original_decision);
    assert_eq!(query(&api, GovernanceQuery::Decision { decision: final_decision_ref }).await, final_decision);
    reopened.stop().await;
    assert_eq!(inventory(&lab, &done, &owner), original_inventory);
    assert_eq!(std::fs::read(notice_ack_path).unwrap(), notice_ack_bytes);
    assert_eq!(std::fs::read(completion_ack_path).unwrap(), completion_ack_bytes);
    assert_eq!(std::fs::read(completion_dir.join("input.json")).unwrap(), input_bytes);
    assert_eq!(std::fs::read(mirror_path).unwrap(), mirror_bytes);
}
