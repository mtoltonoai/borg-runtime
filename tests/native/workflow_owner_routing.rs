//! Real native poser and document-owner consumers. The loopback transport holds
//! the original Begin, never synthesizes a worker report or successful ACK.
//! SQLite observations happen only after both owned processes have stopped.
use super::*;
use borg_runtime::collaboration::{
    documents::{DocumentContent, DocumentMutation, DocumentMutationOutcome, DocumentView},
    notifications::{DeliveryBatch, DeliveryObservation},
    questions::{Question, QuestionAnswer, QuestionQuery, QuestionQueryResult},
    workflow::{ActorProvenance, EventId},
};
use borg_runtime::mcp::Caller;

#[derive(Clone)]
struct Exchange { request: Request, reply: Reply }
struct BeginGate {
    task_id: String, event: EventId,
    prepared: Mutex<Option<(Exchange, DeliveryBatch)>>,
    entered: tokio::sync::Notify, release: tokio::sync::Notify, calls: AtomicU64,
}
struct RoutingProxy {
    url: String, gate: Arc<Mutex<Option<Arc<BeginGate>>>>,
    exchanges: Arc<Mutex<Vec<Exchange>>>, _owner: Background,
}
impl RoutingProxy {
    // Existing Proxy forwarding pattern, local to this child. The original
    // Prepare reply selects the exact terminal EventId and batch to hold.
    async fn start(upstream: String, bound: usize) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let gate = Arc::new(Mutex::new(None::<Arc<BeginGate>>));
        let exchanges = Arc::new(Mutex::new(Vec::<Exchange>::new()));
        let (gates, saved) = (gate.clone(), exchanges.clone());
        let owner = tokio::spawn(async move {
            let client = reqwest::Client::builder().no_proxy().build().unwrap();
            let mut connections = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (stream, _) = accepted.unwrap();
                        let (upstream, client, gates, saved) = (upstream.clone(), client.clone(), gates.clone(), saved.clone());
                        connections.spawn(async move {
                            let service = service_fn(move |request: HttpRequest<Incoming>| {
                                let (upstream, client, gates, saved) = (upstream.clone(), client.clone(), gates.clone(), saved.clone());
                                async move {
                                    let path = request.uri().path().to_owned();
                                    assert!(matches!(path.as_str(), "/mcp" | "/api/operations"));
                                    let caller = request.headers().get("x-borg-caller").cloned();
                                    let version = request.headers().get("mcp-protocol-version").cloned();
                                    let bytes = request.into_body().collect().await.unwrap().to_bytes();
                                    assert!(bytes.len() <= bound);
                                    let operation: Option<Request> = (path == "/api/operations")
                                        .then(|| serde_json::from_slice(&bytes).unwrap());
                                    let gate = { gates.lock().unwrap().clone() };
                                    if let (Some(gate), Some(request)) = (&gate, &operation) {
                                        if let Operation::BeginCollaborationDelivery { caller, batch_id, .. } = &request.command {
                                            let selected = gate.prepared.lock().unwrap().as_ref().is_some_and(|(_, batch)|
                                                caller.task_id == gate.task_id && batch.batch_id == *batch_id
                                                    && batch.page.contains_event(&gate.event));
                                            if selected {
                                                assert_eq!(gate.calls.fetch_add(1, Ordering::SeqCst), 0);
                                                gate.entered.notify_one();
                                                tokio::time::timeout(Duration::from_secs(10), gate.release.notified())
                                                    .await.expect("original Begin not released");
                                            }
                                        }
                                    }
                                    let mut forward = client.post(format!("{upstream}{path}"))
                                        .header("content-type", "application/json").body(bytes.to_vec());
                                    if let Some(caller) = caller { forward = forward.header("x-borg-caller", caller); }
                                    if let Some(version) = version { forward = forward.header("mcp-protocol-version", version); }
                                    let response = forward.send().await.map_err(io::Error::other)?;
                                    let status = response.status();
                                    let bytes = response.bytes().await.map_err(io::Error::other)?;
                                    assert!(bytes.len() <= bound);
                                    if let Some(request) = operation {
                                        if matches!(&request.command, Operation::PrepareCollaborationDelivery { .. }
                                            | Operation::BeginCollaborationDelivery { .. }
                                            | Operation::AcknowledgeCollaborationDelivery { .. }) {
                                            assert!(status.is_success());
                                            let reply: Reply = serde_json::from_slice(&bytes).unwrap();
                                            assert_eq!(reply.request_id, request.request_id);
                                            reply.validate_for(&request.command).unwrap();
                                            let exchange = Exchange { request, reply };
                                            if let (Some(gate), Operation::PrepareCollaborationDelivery { caller, .. }) = (&gate, &exchange.request.command) {
                                                if caller.task_id == gate.task_id {
                                                    if let Outcome::Ok { data: Data::CollaborationDelivery(view) } = &exchange.reply.outcome {
                                                        if let Some(batch) = view.pending.as_ref().filter(|b| b.page.contains_event(&gate.event)) {
                                                            assert!(batch.delivery.is_none());
                                                            assert_eq!(batch.page.routing.as_ref().unwrap().items.iter()
                                                                .filter(|event| event.id == gate.event).count(), 1);
                                                            let mut prepared = gate.prepared.lock().unwrap();
                                                            assert!(prepared.is_none());
                                                            *prepared = Some((exchange.clone(), batch.clone()));
                                                        }
                                                    }
                                                }
                                            }
                                            let mut saved = saved.lock().unwrap();
                                            assert!(saved.len() < 64, "bounded delivery transcript");
                                            saved.push(exchange);
                                        }
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
        Self { url, gate, exchanges, _owner: Background(owner) }
    }
}

struct PostGate { entered: tokio::sync::Notify, release: tokio::sync::Notify }
impl PostGate {
    fn new() -> Arc<Self> { Arc::new(Self { entered: tokio::sync::Notify::new(), release: tokio::sync::Notify::new() }) }
    async fn wait(&self) {
        tokio::time::timeout(Duration::from_secs(30), self.entered.notified()).await.expect("required POST absent");
    }
    async fn hold(&self) {
        self.entered.notify_one();
        tokio::time::timeout(Duration::from_secs(30), self.release.notified()).await.expect("required response not released");
    }
}

fn owner_input(lab: &Lab, agent: &str) -> TaskInput {
    let mut input = lab.input(2);
    input.definition.id = agent.into();
    input.definition.revision = "document-owner-native-v2".into();
    input.definition.execution_profile = Some(ExecutionProfile::CollaborationV2 { publication: None, web_search: None });
    input
}
async fn call_ok(api: &ApiClient, operation: Operation) -> Reply {
    let reply = api.call(operation.clone()).await.unwrap();
    reply.validate_for(&operation).unwrap();
    assert!(matches!(reply.outcome, Outcome::Ok { .. }), "{reply:?}");
    reply
}
async fn create_document(api: &ApiClient, owner: &Task) -> DocumentView {
    let reply = call_ok(api, Operation::MutateDocument { operation_id: "owner-routing-create-document".into(),
        mutation: DocumentMutation::Create {
            content: DocumentContent { title: "Exact native owner target".into(), body: EVENT_TEXT.into() },
            supplied_content_id: None,
            responsible_owner: PrincipalRef::Agent { agent_id: owner.assigned_agent_id.clone() },
        },
    }).await;
    let Outcome::Ok { data: Data::DocumentMutation(DocumentMutationOutcome::Document { view }) } = reply.outcome
    else { panic!("document outcome") };
    view
}
fn pose_arguments(document: &DocumentView) -> Value {
    json!({"question": PoseQuestion {
        record: RecordRef::Document { document_id: document.document.id.clone() },
        recipient: PrincipalRef::TrustedHostOperator, prompt: EVENT_TEXT.into(),
        options: vec![QuestionOption { id: "keep".into(), label: "Keep".into(), explanation: "Keep seven".into() },
            QuestionOption { id: "change".into(), label: "Change".into(), explanation: "Reconsider".into() }],
        recommended_option_id: "keep".into(),
    }})
}
async fn document_question(api: &ApiClient, document: &DocumentView) -> Question {
    let reply = call_ok(api, Operation::QueryQuestion { query: QuestionQuery::List {
        record: Some(RecordRef::Document { document_id: document.document.id.clone() }), status: None, after: None, limit: 8,
    }}).await;
    let Outcome::Ok { data: Data::QuestionQuery(QuestionQueryResult::Questions(page)) } = reply.outcome
    else { panic!("question query outcome") };
    assert_eq!(page.items.len(), 1); assert!(page.next_cursor.is_none());
    page.items[0].question.clone()
}
async fn events(api: &ApiClient) -> Vec<WorkflowActivity> {
    let mut result = Vec::new(); let mut cursor = None;
    for _ in 0..8 {
        let reply = call_ok(api, Operation::QueryWorkflowActivity { cursor, limit: 8 }).await;
        let Outcome::Ok { data: Data::WorkflowActivity(page) } = reply.outcome else { unreachable!() };
        result.extend(page.items); cursor = page.next_cursor;
        if cursor.is_none() { return result; }
    }
    panic!("finite fixture activity bound")
}
async fn resolve(api: &ApiClient, question: &Question) -> (Operation, Reply, WorkflowActivity) {
    let operation = Operation::MutateQuestion { operation_id: "owner-routing-original-answer".into(),
        mutation: QuestionMutation::Answer { question_id: question.id.clone(), expected_revision: question.revision,
            answer: QuestionAnswer { option_id: "keep".into(), explanation: "Original terminal resolution".into() } },
    };
    let reply = call_ok(api, operation.clone()).await; assert!(reply.commit.is_some());
    let closed: Vec<_> = events(api).await.into_iter().filter(|event|
        event.subject == (RecordRef::Question { question_id: question.id.clone() })
            && matches!(&event.detail, ActivityDetail::Question(QuestionChange::Closed { .. }))).collect();
    assert_eq!(closed.len(), 1); let event = closed.into_iter().next().unwrap();
    let route = event.routing.as_ref().unwrap();
    assert_eq!(route.requested_by, question.poser); assert_eq!(route.linked_record, question.posed.record);
    (operation, reply, event)
}
fn caller(task: &Task) -> Caller {
    let a = task.attempt.as_ref().unwrap();
    Caller { task_id: task.id.clone(), attempt_id: a.id.clone(), generation: a.generation,
        worker_id: a.worker_id.clone(), incarnation: a.worker_incarnation.clone() }
}
fn notices(request: &Value) -> Vec<Value> {
    request["input"].as_array().unwrap().iter().filter(|item| item["role"] == "developer")
        .map(|item| serde_json::from_str(item["content"].as_str().unwrap()).unwrap()).collect()
}
fn occurrences(request: &Value, event: &WorkflowActivity) -> usize {
    notices(request).iter().flat_map(|notice| notice["events"].as_array().unwrap())
        .filter(|item| item["id"] == json!(event.id)).map(|item| { assert_eq!(item, &json!(event)); 1 }).sum()
}
async fn terminal(api: &ApiClient, task: &Task) -> Task {
    let terminal = until(api, &task.id, |current| current.status == TaskStatus::Succeeded
        && current.attempt.as_ref().is_some_and(|attempt| attempt.stopped)).await;
    assert_eq!(terminal.input, task.input); assert_eq!(terminal.session_id, task.session_id);
    assert_eq!(terminal.assigned_agent_id, task.assigned_agent_id);
    assert_eq!(terminal.result.as_ref().unwrap().output, Some(json!(7)));
    assert!(!terminal.attempt.as_ref().unwrap().uncertain); terminal
}
fn delivery(reply: &Reply) -> &DeliveryObservation {
    let Outcome::Ok { data: Data::CollaborationDelivery(value) } = &reply.outcome else { panic!("{reply:?}") }; value
}
async fn assert_receiving(api: &ApiClient, lab: &Lab, task: &Task, peer: &Peer, event: &WorkflowActivity) -> DeliveryBatch {
    let active = get(api, &task.id).await; assert_eq!(active.status, TaskStatus::Active);
    assert_eq!(active.input, task.input); assert_eq!(active.session_id, task.session_id);
    let observation = observe_delivery(api, &active).await;
    let batch = observation.pending.as_ref().expect("actual Prepare and Begin precede POST");
    assert_eq!(batch.page.routing.as_ref().unwrap().items, vec![event.clone()]);
    assert!(batch.page.items.is_empty() && batch.page.continuation.is_none());
    let intent = batch.delivery.as_ref().unwrap(); assert_eq!(intent.caller, caller(&active));
    assert_eq!(intent.context_binding, active.attempt.as_ref().unwrap().id);
    let durable = mirror(lab, task);
    assert_eq!(durable["pending"]["batch"], json!(batch));
    assert_eq!(durable["pending"]["delivery"]["appended"], false);
    let requests = peer.requests.lock().unwrap().clone(); assert_eq!(requests.len(), 6);
    let exact_text = durable["pending"]["text"].as_str().unwrap();
    let delivered: Vec<_> = requests[5]["input"].as_array().unwrap().iter()
        .filter(|item| item["role"] == "developer" && item["content"] == exact_text).collect();
    assert_eq!(delivered.len(), 1);
    let notice: Value = serde_json::from_str(exact_text).unwrap();
    assert_eq!(notice["batch_id"], batch.batch_id);
    assert_eq!(notice["events"], json!([event]));
    assert_eq!(requests[5]["input"][0]["content"], task.input.instruction);
    let outputs: Vec<_> = requests[5]["input"].as_array().unwrap().iter()
        .filter(|item| item["type"] == "function_call_output").collect();
    assert_eq!(outputs.len(), 1);
    let tool_result: Value = serde_json::from_str(outputs[0]["output"].as_str().unwrap()).unwrap();
    let retained: Value = serde_json::from_slice(&std::fs::read(lab.state().join("collaboration")
        .join(process::component(&active.attempt.as_ref().unwrap().id)).join("0000000001.json")).unwrap()).unwrap();
    assert_eq!(retained["consumed"], true);
    assert_eq!(tool_result["outcome"], retained["outcome"]);
    assert_eq!(tool_result["commit"], retained["outcome_context"]["commit"]);
    assert_eq!(outputs[0]["call_id"], "call-4");
    assert_eq!(reported_requests(api, &task.id, &active.attempt.as_ref().unwrap().id).await, 2);
    batch.clone()
}
fn successful_ack(exchange: &Exchange, batch: &str) -> bool {
    matches!(&exchange.request.command, Operation::AcknowledgeCollaborationDelivery { batch_id, .. } if batch_id == batch)
        && matches!(&exchange.reply.outcome, Outcome::Ok { .. })
}
// Invoke only with worker and controller physically closed.
fn assert_stored_exchanges(lab: &Lab, exchanges: &[Exchange]) {
    let c = rusqlite::Connection::open_with_flags(lab.root.join("tasks.sqlite"), rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    for exchange in exchanges {
        let id = exchange.request.command.mutation_id().unwrap();
        let (request, outcome, revision): (String, String, i64) = c.query_row(
            "SELECT request,outcome,controller_revision FROM receipts WHERE id=?1", [id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))).unwrap();
        assert_eq!(serde_json::from_str::<Value>(&request).unwrap(), json!(exchange.request.command));
        assert_eq!(serde_json::from_str::<Outcome>(&outcome).unwrap(), exchange.reply.outcome);
        assert_eq!(exchange.reply.commit.as_ref().unwrap().controller_revision, revision as u64);
    }
}
async fn exact_replay(api: &ApiClient, operation: &Operation, original: &Reply) {
    let replay = api.call(operation.clone()).await.unwrap(); replay.validate_for(operation).unwrap();
    assert_eq!(replay.outcome, original.outcome); assert_eq!(replay.commit, original.commit);
}

fn closed_commands(lab: &Lab, tasks: &[&Task]) -> Vec<Value> {
    let c = rusqlite::Connection::open_with_flags(lab.root.join("tasks.sqlite"), rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    assert_eq!(c.query_row("SELECT count(*) FROM tasks", [], |row| row.get::<_, i64>(0)).unwrap(), 3);
    assert_eq!(c.query_row("SELECT count(*) FROM sessions", [], |row| row.get::<_, i64>(0)).unwrap(), 3);
    assert_eq!(c.query_row("SELECT count(*) FROM attempts", [], |row| row.get::<_, i64>(0)).unwrap(), 3);
    let mut statement = c.prepare("SELECT body FROM commands ORDER BY seq LIMIT 32").unwrap();
    let rows = statement.query_map([], |row| row.get::<_, String>(0)).unwrap()
        .map(|row| serde_json::from_str::<Value>(&row.unwrap()).unwrap()).collect::<Vec<_>>();
    assert!(rows.len() < 32);
    let mut runs = 0;
    for row in &rows {
        if let WorkerCommand::RunAttempt { attempt, session, task, .. } = serde_json::from_value(row.clone()).unwrap() {
            let original = tasks.iter().find(|original| original.id == attempt.task_id).unwrap();
            assert_eq!(session.id, original.session_id);
            assert_eq!(task, original.input);
            assert_eq!(attempt.id, original.attempt.as_ref().unwrap().id);
            runs += 1;
        }
    }
    assert_eq!(runs, 3, "no event-created task, session, attempt or Run command");
    rows
}

// Six fixed task-required requests: poser tool/final, wrong or old owner
// tool/final, rightful owner tool/final. No adaptive scan or notice-only turn.
async fn owner_join(change_owner: bool) {
    let lab = workflow_lab(if change_owner { "native-document-owner-change" } else { "native-inactive-asker-owner" });
    let mut controller = lab.controller("controller").await;
    let api = ApiClient::new(&lab.url, lab.limits.clone()).unwrap();
    let asker = submit_identified(&api, owner_input(&lab, "native-bound-asker"), "submit-native-asker").await;
    let first = submit_identified(&api, owner_input(&lab, if change_owner { "document-old-owner" }
        else { "unrelated-native-owner" }), "submit-first-consumer").await;
    let mut rightful = if change_owner { None } else {
        Some(submit_identified(&api, owner_input(&lab, "document-current-owner"), "submit-rightful-consumer").await)
    };
    let document = create_document(&api, rightful.as_ref().unwrap_or(&first)).await;
    let (first_post, second_post, routed_post) = (PostGate::new(), PostGate::new(), PostGate::new());
    let arguments = pose_arguments(&document);
    let peer = Peer::start_async({
        let (first_post, second_post, routed_post) = (first_post.clone(), second_post.clone(), routed_post.clone());
        move |index, request| {
            let (first_post, second_post, routed_post, arguments) = (first_post.clone(), second_post.clone(), routed_post.clone(), arguments.clone());
            async move {
                match index {
                    0 => Some(function(index, "collab_question_pose", arguments)),
                    1 => {
                        let outputs: Vec<_> = request["input"].as_array().unwrap().iter()
                            .filter(|item| item["type"] == "function_call_output").collect();
                        assert_eq!(outputs.len(), 1); assert_eq!(outputs[0]["call_id"], "call-0");
                        let posed: Value = serde_json::from_str(outputs[0]["output"].as_str().unwrap()).unwrap();
                        assert_eq!(posed["outcome"]["status"], "ok");
                        Some(final_value())
                    }
                    2 => { first_post.hold().await; Some(function(index, "collab_memory_get", json!({"key":"first-owner-boundary"}))) }
                    3 => { second_post.hold().await; Some(final_value()) }
                    4 => Some(function(index, "collab_memory_get", json!({"key":"rightful-owner-boundary"}))),
                    5 => { routed_post.hold().await; Some(final_value()) }
                    _ => panic!("resolution cannot create a seventh model request"),
                }
            }
        }
    }).await;
    let proxy = RoutingProxy::start(lab.url.clone(), lab.limits.transport.body_bytes).await;
    let mut worker = lab.worker(&proxy.url, &peer.endpoint, "document-routing-worker");
    first_post.wait().await;
    let retired = terminal(&api, &asker).await;
    let question = document_question(&api, &document).await;
    let a = retired.attempt.as_ref().unwrap();
    assert_eq!(question.poser.provenance, ActorProvenance::BoundAgentSession {
        agent_id: asker.assigned_agent_id.clone(), session_id: asker.session_id.clone(), task_id: asker.id.clone(),
        attempt_id: a.id.clone(), generation: a.generation, worker_id: a.worker_id.clone(), incarnation: a.worker_incarnation.clone(),
    });
    assert_eq!(question.posed.record, RecordRef::Document { document_id: document.document.id.clone() });
    let active_first = get(&api, &first.id).await; assert_eq!(active_first.status, TaskStatus::Active);
    let (answer_operation, answered, original) = resolve(&api, &question).await;
    assert_eq!(peer.count(), 3, "answer alone creates no provider request");
    assert_eq!(get(&api, &asker.id).await, retired, "retired asker stays physically stopped");
    let mut superseded = None; let mut manual_refusal = None;
    if change_owner {
        let gate = Arc::new(BeginGate { task_id: first.id.clone(), event: original.id.clone(), prepared: Mutex::new(None),
            entered: tokio::sync::Notify::new(), release: tokio::sync::Notify::new(), calls: AtomicU64::new(0) });
        *proxy.gate.lock().unwrap() = Some(gate.clone()); first_post.release.notify_one();
        tokio::time::timeout(Duration::from_secs(30), gate.entered.notified()).await.expect("routed Begin absent");
        let (prepared, old_batch) = gate.prepared.lock().unwrap().clone().unwrap();
        assert_eq!(delivery(&prepared.reply).pending.as_ref(), Some(&old_batch)); assert!(old_batch.delivery.is_none());
        assert_eq!(old_batch.page.routing.as_ref().unwrap().items, vec![original.clone()]);
        assert_eq!(peer.count(), 3, "no request between actual Prepare and original Begin");
        let next = submit_identified(&api, owner_input(&lab, "document-replacement-owner"), "submit-replacement-consumer").await;
        assert_eq!(next.status, TaskStatus::Queued); assert!(next.attempt.is_none());
        call_ok(&api, Operation::MutateDocument { operation_id: "change-document-owner-before-begin".into(),
            mutation: DocumentMutation::SetResponsibleOwner { document_id: document.document.id.clone(),
                expected_revision: document.document.revision,
                responsible_owner: PrincipalRef::Agent { agent_id: next.assigned_agent_id.clone() } },
        }).await;
        rightful = Some(next); gate.release.notify_one(); second_post.wait().await;
        let old = get(&api, &first.id).await;
        assert_eq!(old.status, TaskStatus::Active, "negative ACK is not a terminal-caller refusal");
        assert_eq!(old.attempt.as_ref().unwrap().id, active_first.attempt.as_ref().unwrap().id);
        let begun = proxy.exchanges.lock().unwrap().iter().find(|exchange|
            matches!(&exchange.request.command, Operation::BeginCollaborationDelivery { batch_id, .. }
                if batch_id == &old_batch.batch_id)).cloned().unwrap();
        let view = delivery(&begun.reply); assert!(view.pending.is_none());
        assert_eq!(view.acknowledged_cursor, delivery(&prepared.reply).acknowledged_cursor);
        let no_append = view.superseded.as_ref().expect("old Begin commits no append");
        assert_eq!(no_append.batch_id, old_batch.batch_id); assert_eq!(no_append.caller, caller(&old));
        assert_eq!(no_append.operation_id, begun.request.command.mutation_id().unwrap());
        assert_eq!(no_append.context_binding, old.attempt.as_ref().unwrap().id);
        let durable = mirror(&lab, &first); assert!(durable["pending"].is_null());
        assert_eq!(durable["superseded"], json!(no_append));
        let wrong_ack = Operation::AcknowledgeCollaborationDelivery { operation_id: "old-owner-correct-batch-wrong-recipient".into(),
            caller: caller(&old), batch_id: old_batch.batch_id.clone(), context_binding: no_append.context_binding.clone() };
        let refused = api.call(wrong_ack.clone()).await.unwrap(); refused.validate_for(&wrong_ack).unwrap();
        assert!(matches!(&refused.outcome, Outcome::Refused { code, .. } if code == "not_found"));
        assert_eq!(observe_delivery(&api, &old).await.acknowledged_cursor, view.acknowledged_cursor);
        assert_eq!(gate.calls.load(Ordering::SeqCst), 1);
        superseded = Some((prepared, begun, old_batch)); manual_refusal = Some((wrong_ack, refused));
    } else { first_post.release.notify_one(); second_post.wait().await; }
    assert_eq!(peer.count(), 4);
    assert!(peer.requests.lock().unwrap().iter().all(|request| occurrences(request, &original) == 0),
        "retired asker and wrong/old owner receive no original terminal occurrence");
    let rightful = rightful.unwrap();
    assert_eq!(get(&api, &rightful.id).await.status, TaskStatus::Queued);
    assert!(proxy.exchanges.lock().unwrap().iter().all(|exchange|
        !matches!(&exchange.request.command, Operation::BeginCollaborationDelivery { caller, .. } if caller.task_id == rightful.id)));
    second_post.release.notify_one(); routed_post.wait().await;
    let first_terminal = terminal(&api, &first).await;
    let batch = assert_receiving(&api, &lab, &rightful, &peer, &original).await;
    assert!(!proxy.exchanges.lock().unwrap().iter().any(|exchange| successful_ack(exchange, &batch.batch_id)),
        "durable pending input and POST precede actual ACK");
    assert_eq!(peer.requests.lock().unwrap().iter().map(|request| occurrences(request, &original)).sum::<usize>(), 1);
    routed_post.release.notify_one(); let rightful_terminal = terminal(&api, &rightful).await;
    assert_eq!(get(&api, &asker.id).await, retired); assert_eq!(peer.count(), 6);
    worker.stop().await; controller.stop().await;
    let exchanges = proxy.exchanges.lock().unwrap().clone();
    let acks: Vec<_> = exchanges.iter().filter(|exchange| successful_ack(exchange, &batch.batch_id)).collect();
    assert_eq!(acks.len(), 1, "one successful native input ACK for the original event"); let ack = acks[0];
    let intent = batch.delivery.as_ref().unwrap();
    let Operation::AcknowledgeCollaborationDelivery { caller: ack_caller, context_binding, .. } = &ack.request.command else { unreachable!() };
    assert_eq!(ack_caller, &intent.caller); assert_eq!(context_binding, &intent.context_binding);
    assert!(delivery(&ack.reply).pending.is_none()); assert_eq!(delivery(&ack.reply).acknowledged_cursor, batch.page.to_cursor);
    if let Some((_, _, old_batch)) = &superseded { assert!(!exchanges.iter().any(|exchange| successful_ack(exchange, &old_batch.batch_id))); }
    assert_stored_exchanges(&lab, &exchanges);
    let command_snapshot = closed_commands(&lab, &[&retired, &first_terminal, &rightful_terminal]);
    for task in [&retired, &first_terminal, &rightful_terminal] { assert_eq!(lab.usage(&task.attempt.as_ref().unwrap().id).0, 2); }
    let states: Vec<_> = [&asker, &first, &rightful].into_iter().map(|task| delivery_state(&lab, &task.session_id)).collect();
    let completed_mirror = mirror(&lab, &rightful); assert!(completed_mirror["pending"].is_null());
    assert_eq!(completed_mirror["acknowledged_cursor"], json!(batch.page.to_cursor));
    let ack_journal: Value = serde_json::from_slice(&std::fs::read(notice_directory(&lab, &rightful).join("acknowledge.json")).unwrap()).unwrap();
    assert_eq!(ack_journal["request"], json!(ack.request.command)); assert_eq!(ack_journal["outcome"], json!(ack.reply.outcome));
    assert_eq!(ack_journal["consumed"], true);
    let mut reopened = lab.controller("controller-reopened").await;
    exact_replay(&api, &answer_operation, &answered).await;
    for exchange in &exchanges { exact_replay(&api, &exchange.request.command, &exchange.reply).await; }
    if let Some((operation, reply)) = &manual_refusal { exact_replay(&api, operation, reply).await; }
    assert_eq!(get(&api, &asker.id).await, retired); assert_eq!(get(&api, &first.id).await, first_terminal);
    assert_eq!(get(&api, &rightful.id).await, rightful_terminal);
    assert_eq!(events(&api).await.iter().find(|event| event.id == original.id), Some(&original));
    assert_eq!(peer.count(), 6, "replay/reopen cannot create a second continuation");
    reopened.stop().await;
    let restored: Vec<_> = [&asker, &first, &rightful].into_iter().map(|task| delivery_state(&lab, &task.session_id)).collect();
    assert_eq!(restored, states); assert_eq!(mirror(&lab, &rightful), completed_mirror);
    assert_eq!(closed_commands(&lab, &[&retired, &first_terminal, &rightful_terminal]), command_snapshot);
}

//= spec/impl/v2/implementation.md#committed-resolutions-reach-their-consumers
//# An answer delivery MUST target its active asker or otherwise the record's current responsible owner.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inactive_native_asker_resolution_reaches_current_document_owner_once() { Box::pin(owner_join(false)).await; }

//= spec/impl/v2/implementation.md#committed-resolutions-reach-their-consumers
//# A repeated resolution delivery MUST NOT start a second continuation for the same resolution occurrence.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn document_owner_change_after_native_prepare_supersedes_old_begin_and_routes_once() { Box::pin(owner_join(true)).await; }
