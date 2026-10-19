//! One actual same-parent V2 journey, nested under workflow_join.rs.
//! The configured Coding task, worker, checkpoint and acceptance are real. Its
//! app-server peer is the existing controlled codex_protocol.py fixture, NOT an
//! installed/pinned Codex or an external provider. Six native POSTs plus the
//! one-request child reservation exhaust the accepted allowance of seven.
use super::*;
use borg_runtime::collaboration::documents::*;
use borg_runtime::collaboration::questions::{QuestionQuery, QuestionQueryResult};
use borg_runtime::collaboration::work::{WaitStatus, WorkQuery, WorkQueryResult, WorkView};
use borg_runtime::collaboration::workflow::{ActorProvenance, AnswerRef, QuestionResolution,
    ResolverOutcome, ResolverRef, ReviewTarget, TrustedHostPath, WorkflowContinuation};
use std::os::unix::fs::PermissionsExt;

fn git_at(root: &Path, args: &[&str]) -> String {
    let output = Command::new("/usr/bin/git").args(["-c", "core.hooksPath=/dev/null",
        "-c", "user.name=Journey Fixture", "-c", "user.email=journey@example.invalid"])
        .args(args).current_dir(root).output().unwrap();
    assert!(output.status.success(), "fixture git: {}", String::from_utf8_lossy(&output.stderr));
    String::from_utf8(output.stdout).unwrap().trim().into()
}
fn tool_outcome(request: &Value, call: &str) -> Outcome {
    let outputs: Vec<_> = request["input"].as_array().unwrap().iter()
        .filter(|v| v["type"] == "function_call_output" && v["call_id"] == call).collect();
    assert_eq!(outputs.len(), 1, "one retained tool result, even beside a workflow notice");
    let value: Value = serde_json::from_str(outputs[0]["output"].as_str().unwrap()).unwrap();
    assert!(value["commit"].is_object(), "real committed tool result");
    serde_json::from_value(value["outcome"].clone()).unwrap()
}
async fn work(api: &ApiClient, id: &str) -> WorkView {
    let op = Operation::QueryWork { query: WorkQuery::Get { task_id: id.into() } };
    let reply = api.call(op.clone()).await.unwrap(); reply.validate_for(&op).unwrap();
    let Outcome::Ok { data: Data::WorkQuery(WorkQueryResult::Task(view)) } = reply.outcome
    else { panic!("work: {reply:?}") }; view
}
async fn question(api: &ApiClient, id: &str) -> borg_runtime::collaboration::questions::QuestionView {
    let op = Operation::QueryQuestion { query: QuestionQuery::Get { question_id: id.into() } };
    let reply = api.call(op.clone()).await.unwrap(); reply.validate_for(&op).unwrap();
    let Outcome::Ok { data: Data::QuestionQuery(result) } = reply.outcome
    else { panic!("question: {reply:?}") };
    let QuestionQueryResult::Question(view) = *result else { panic!("question: {result:?}") }; view
}
async fn entered(signal: &tokio::sync::Notify) {
    tokio::time::timeout(Duration::from_secs(30), signal.notified()).await.unwrap();
}
fn receipt(c: &rusqlite::Connection, id: &str) -> (Operation, Outcome, u64) {
    let (request, outcome, revision): (String, String, i64) = c.query_row(
        "SELECT request,outcome,controller_revision FROM receipts WHERE id=?1", [id],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))).unwrap();
    assert!(revision > 0);
    (serde_json::from_str(&request).unwrap(), serde_json::from_str(&outcome).unwrap(), revision as u64)
}
fn segment(value: &str) -> String {
    value.bytes().map(|b| if b.is_ascii_alphanumeric() || b"-._~".contains(&b) {
        char::from(b).to_string()
    } else { format!("%{b:02X}") }).collect()
}
fn same_parent(original: &Task, current: &Task) {
    assert_eq!(current.id, original.id); assert_eq!(current.session_id, original.session_id);
    assert_eq!(current.assigned_agent_id, original.assigned_agent_id);
    assert_eq!(current.input, original.input);
    let attempt = current.attempt.as_ref().unwrap();
    assert_eq!(attempt.session_id, original.session_id);
    assert_eq!(attempt.deadline_unix_ms, original.input.limits.deadline_unix_ms);
    assert!(!attempt.uncertain);
}
fn bound_actor(actor: &borg_runtime::collaboration::workflow::RecordedActor, task: &Task) {
    assert_eq!(actor.principal, PrincipalRef::Agent { agent_id: task.assigned_agent_id.clone() });
    let a = task.attempt.as_ref().unwrap();
    assert_eq!(actor.provenance, ActorProvenance::BoundAgentSession {
        agent_id: task.assigned_agent_id.clone(), session_id: task.session_id.clone(),
        task_id: task.id.clone(), attempt_id: a.id.clone(), generation: a.generation,
        worker_id: a.worker_id.clone(), incarnation: a.worker_incarnation.clone(),
    });
}

// Gate: spec/impl/v2/execution-plan.md#V2_COLLAB_JOIN — JOURNEY
// Actual agent delegates work, poses a routed blocking question, commits the
// wait, releases its slot after stopping, receives committed answer once,
// resumes under its original session association, and submits exact document
// version or code/result reference for review.
//= spec/impl/v2/implementation.md#collaboration-waits-are-separate-from-execution-prerequisites
//# A requested collaboration wait MUST be committed before initiating the current attempt's quiescence.
//# A task suspended on a collaboration wait MUST release its execution slot after its current execution has stopped.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]

// Traceability scope: Actual native work and its configured child run before an explicit wait; the controlled coding peer and accepted inputs remain scoped.
//= spec/impl/v2/implementation.md#collaboration-surfaces-preserve-existing-authority
//= type=test
//# The collaboration expansion MUST NOT require human approval of ordinary work that has no explicitly requested product wait.
//= spec/impl/v2/implementation.md#delegation-is-a-task-handoff
//= type=test
//# A delegation request MUST NOT change configured host, repository or provider access.

async fn native_delegated_parent_questions_waits_and_reviews_one_exact_document() {
    Box::pin(journey()).await;
}
async fn journey() {
    let mut lab = workflow_lab("delegation-question-review");
    let mut limits = (*lab.limits).clone(); limits.worker.coding_capacity = 1;
    limits.validate().unwrap(); lab.limits = Arc::new(limits);
    std::fs::write(&lab.worker_limits_file, serde_json::to_vec_pretty(&*lab.limits).unwrap()).unwrap();
    let seed = lab.root.join("seed"); std::fs::create_dir(&seed).unwrap();
    std::fs::write(seed.join("answer.txt"), "before\n").unwrap();
    git_at(&seed, &["init", "-q"]); git_at(&seed, &["add", "answer.txt"]);
    git_at(&seed, &["commit", "-qm", "original controlled source"]);
    let start_commit = git_at(&seed, &["rev-parse", "HEAD"]);
    let remote = lab.root.join("remote.git");
    git_at(&lab.root, &["clone", "--bare", "--quiet", seed.to_str().unwrap(), remote.to_str().unwrap()]);
    let gate = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let coding_peer = lab.root.join("codex_protocol.py");
    std::fs::write(&coding_peer, include_str!("../recovery/fixtures/codex_protocol.py")).unwrap();
    std::fs::set_permissions(&coding_peer, std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::write(coding_peer.with_extension("json"), json!({"port":gate.local_addr().unwrap().port()}).to_string()).unwrap();
    let mut controller = lab.controller("controller").await;
    let api = ApiClient::new(&lab.url, lab.limits.clone()).unwrap();
    let registration = borg_runtime::repository_catalogue::Revision {
        repository_id: "journey-source".into(), registration_revision: "1".into(),
    };
    let registered = api.call(Operation::RegisterRepository {
        operation_id: "journey-register-repository".into(),
        registration: borg_runtime::repository_catalogue::Registration {
            pull_request_target: None, repository_id: registration.repository_id.clone(),
            registration_revision: registration.registration_revision.clone(),
            display_name: "Journey source".into(), source_endpoint: remote.display().to_string(),
            checkpoint_endpoint: remote.display().to_string(), source_store_key: "remote.git".into(),
            checkpoint_store_key: "remote.git".into(),
        },
    }).await.unwrap();
    assert!(matches!(registered.outcome, Outcome::Ok { data: Data::RepositoryRegistration(_) }));
    let mut input = lab.input(7);
    input.definition.execution_profile = Some(ExecutionProfile::CollaborationV2 { publication: None, web_search: None });
    input.definition.revision = "delegation-question-review-v2".into();
    input.instruction = "Delegate the configured source correction. After its accepted checkpoint, ask the operator whether to review it, wait, then create its review document and request review; return seven.".into();
    input.native_workflow = Some(Box::new(NativeWorkflow::CodingThenContinue {
        child: CodingTaskTemplate {
            instruction_source: None, pull_request: None, instruction_prefix: "Correct only answer.txt.".into(),
            definition: Definition { charter: None, limit_overrides: None, inference: Some(InferenceSettings::selected()),
                execution_profile: Some(ExecutionProfile::CollaborationV1 { web_search: None }),
                id: "journey-coding-child".into(), revision: "1".into(), executor: ExecutorKind::Coding },
            repository: Repository { pull_request_target: None, registration: Some(registration.clone()),
                url: String::new(), checkpoint_remote: String::new(), start_commit },
            acceptance_commands: vec![vec!["/bin/sh".into(), "-c".into(), "test \"$(cat answer.txt)\" = after".into()]],
            dependencies: vec![], limits: Limits { deadline_unix_ms: input.limits.deadline_unix_ms,
                model_requests: 1, output_bytes: 4096, result_bytes: 8192, max_delegation_depth: 0 },
        },
    }));
    let parent = submit(&api, input).await;
    let child_entered = Arc::new(tokio::sync::Notify::new());
    let child_release = Arc::new(tokio::sync::Notify::new());
    let answer_entered = Arc::new(tokio::sync::Notify::new());
    let answer_release = Arc::new(tokio::sync::Notify::new());
    let review_entered = Arc::new(tokio::sync::Notify::new());
    let review_release = Arc::new(tokio::sync::Notify::new());
    let document_body = Arc::new(Mutex::new(None::<String>));
    let posed = Arc::new(Mutex::new(None::<borg_runtime::collaboration::questions::Question>));
    let document = Arc::new(Mutex::new(None::<DocumentView>));
    let review = Arc::new(Mutex::new(None::<ReviewView>));
    let peer = Peer::start_async({
        let parent = parent.clone(); let child_entered = child_entered.clone(); let child_release = child_release.clone();
        let answer_entered = answer_entered.clone(); let answer_release = answer_release.clone();
        let review_entered = review_entered.clone(); let review_release = review_release.clone();
        let document_body = document_body.clone(); let posed = posed.clone();
        let document = document.clone(); let review = review.clone();
        move |index, request| {
            let parent = parent.clone(); let child_entered = child_entered.clone(); let child_release = child_release.clone();
            let answer_entered = answer_entered.clone(); let answer_release = answer_release.clone();
            let review_entered = review_entered.clone(); let review_release = review_release.clone();
            let document_body = document_body.clone(); let posed = posed.clone();
            let document = document.clone(); let review = review.clone();
            async move { Some(match index {
                0 => function(index, "collab_task_delegate", json!({"instruction":"Set answer.txt to after."})),
                1 => {
                    child_entered.notify_one(); child_release.notified().await;
                    function(index, "collab_question_pose", json!({"question":PoseQuestion {
                        record: RecordRef::Task { task_id: parent.id.clone() }, recipient: PrincipalRef::TrustedHostOperator,
                        prompt: "Should the accepted child checkpoint be submitted for review?".into(),
                        options: vec![QuestionOption { id:"review".into(), label:"Review".into(), explanation:"Review the accepted checkpoint.".into() },
                            QuestionOption { id:"hold".into(), label:"Hold".into(), explanation:"Do not submit the review.".into() }],
                        recommended_option_id:"review".into(),
                    }}))
                },
                2 => {
                    let Outcome::Ok { data: Data::QuestionMutation(changed) } = tool_outcome(&request, "call-1")
                    else { panic!("actual question pose failed") };
                    let q = changed.question.question; *posed.lock().unwrap() = Some(q.clone());
                    function(index, "collab_task_wait", json!({"task_id":parent.id,"expected_revision":0,
                        "resolver":{"kind":"question","question_id":q.id}}))
                },
                3 => {
                    answer_entered.notify_one(); answer_release.notified().await;
                    function(index, "collab_document_create", json!({"content":{"title":"Accepted child checkpoint review",
                        "body":document_body.lock().unwrap().clone().unwrap()},"supplied_content_id":null,
                        "responsible_owner":{"kind":"agent","agent_id":parent.assigned_agent_id}}))
                },
                4 => {
                    let Outcome::Ok { data: Data::DocumentMutation(DocumentMutationOutcome::Document { view }) } = tool_outcome(&request, "call-3")
                    else { panic!("actual document creation failed") };
                    *document.lock().unwrap() = Some(view.clone());
                    function(index, "collab_review_create", json!({"target":ReviewTarget::DocumentVersion { version:view.version.reference },
                        "reviewer":PrincipalRef::TrustedHostOperator,"summary":"Review the exact document describing the accepted child checkpoint."}))
                },
                5 => {
                    let Outcome::Ok { data: Data::ReviewMutation(result) } = tool_outcome(&request, "call-4")
                    else { panic!("actual review request failed") };
                    let ReviewMutationOutcome::Review(view) = *result else { panic!("actual review request failed") };
                    *review.lock().unwrap() = Some(view); review_entered.notify_one(); review_release.notified().await;
                    final_value()
                },
                _ => panic!("the journey permits exactly six native POSTs"),
            }) }
        }
    }).await;
    let mut worker = lab.worker_with_codex(&lab.url, &peer.endpoint, "journey-worker", &coding_peer);
    let (mut coding, _) = tokio::time::timeout(Duration::from_secs(30), gate.accept()).await.unwrap().unwrap();
    let observation = tokio::time::timeout(Duration::from_secs(30), async {
        let mut observation = Vec::new();
        loop { let byte = coding.read_u8().await.unwrap(); if byte == b'\n' { break; }
            observation.push(byte); assert!(observation.len() <= 4096); }
        observation
    }).await.unwrap();
    let coding_observation: Value = serde_json::from_slice(&observation).unwrap();
    assert_eq!(coding_observation["before"], "before\n");
    let delegated = until(&api, &parent.id, |t| t.waiting_child_id.is_some()
        && t.attempt.as_ref().is_some_and(|a| a.stopped)).await;
    same_parent(&parent, &delegated); let first = delegated.attempt.clone().unwrap();
    assert_eq!(peer.count(), 1); assert_eq!(reported_requests(&api, &parent.id, &first.id).await, 1);
    let child_id = delegated.waiting_child_id.clone().unwrap();
    let child = get(&api, &child_id).await;
    assert_eq!(child.parent_task_id.as_deref(), Some(parent.id.as_str()));
    assert_ne!(child.session_id, parent.session_id);
    coding.write_all(b"G").await.unwrap(); drop(coding);
    entered(&child_entered).await;
    let accepted_child = until(&api, &child_id, |t| t.status == TaskStatus::Succeeded
        && t.attempt.as_ref().is_some_and(|a| a.stopped)).await;
    let child_result = accepted_child.result.as_ref().unwrap();
    assert_eq!(accepted_child.input, child.input); assert_eq!(accepted_child.session_id, child.session_id);
    let checkpoint = child_result.checkpoint.as_ref().expect("actual accepted coding checkpoint");
    assert_eq!(checkpoint.registration, registration);
    assert_eq!(git_at(&remote, &["rev-parse", &checkpoint.remote_ref]), checkpoint.commit);
    assert_eq!(git_at(&remote, &["show", &format!("{}:answer.txt", checkpoint.commit)]), "after");
    assert!(!child_result.validation.is_empty());
    let continued = get(&api, &parent.id).await; same_parent(&parent, &continued);
    let second = continued.attempt.clone().unwrap(); assert_eq!(second.generation, first.generation + 1);
    assert_eq!(continued.completed_child.as_ref().unwrap().task_id, child_id);
    assert_eq!(continued.completed_child.as_ref().unwrap().status, TaskStatus::Succeeded);
    assert_eq!(continued.completed_child.as_ref().unwrap().result.as_ref(), accepted_child.result.as_deref());
    let expected_instruction = borg_runtime::delegation::continuation_instruction(&parent.input,
        continued.completed_child.as_ref().unwrap()).unwrap();
    assert_eq!(peer.requests.lock().unwrap()[1]["input"][0]["content"], expected_instruction);
    *document_body.lock().unwrap() = Some(format!("Parent {} received child {} accepted checkpoint {} at {}. The operator chose review.",
        parent.id, child_id, checkpoint.commit, checkpoint.remote_ref));
    child_release.notify_one();
    let waiting = until(&api, &parent.id, |t| t.status == TaskStatus::Queued
        && t.attempt.as_ref().is_some_and(|a| a.stopped && a.id == second.id)).await;
    same_parent(&parent, &waiting); assert_eq!(peer.count(), 3);
    assert_eq!(reported_requests(&api, &parent.id, &second.id).await, 2);
    let blocked = work(&api, &parent.id).await;
    assert!(blocked.coordination_blocked); assert_eq!(blocked.open_waits, 1);
    assert_eq!(blocked.waits.items.len(), 1); assert!(blocked.waits.next_cursor.is_none());
    let wait = blocked.waits.items[0].clone(); assert_eq!(wait.status, WaitStatus::Open);
    let q = posed.lock().unwrap().clone().unwrap();
    assert_eq!(wait.resolver, ResolverRef::Question { question_id:q.id.clone() });
    bound_actor(&q.poser, &continued);
    let browser = reqwest::Client::builder().no_proxy().redirect(reqwest::redirect::Policy::none()).build().unwrap();
    let question_url = format!("{}/questions/{}", lab.url, segment(&q.id));
    let html = browser.get(&question_url).send().await.unwrap(); assert_eq!(html.status(), reqwest::StatusCode::OK);
    let html = html.text().await.unwrap(); assert!(html.contains(&q.id)
        && html.contains("Should the accepted child checkpoint be submitted for review?"));
    let answered = browser.post(format!("{question_url}/answer")).header("Origin", &lab.url)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(format!("operation_id=journey-browser-answer&expected_revision={}&option_id=review&explanation={}",
            q.revision, segment("Review the accepted child checkpoint.")))
        .send().await.unwrap();
    assert_eq!(answered.status(), reqwest::StatusCode::SEE_OTHER);
    let answer_view = question(&api, &q.id).await;
    let answer = answer_view.active_answer.as_ref().unwrap();
    assert_eq!(answer.answer.option_id, "review");
    assert_eq!(answer.author.provenance, ActorProvenance::TrustedHost { path:TrustedHostPath::Browser });
    entered(&answer_entered).await;
    let resumed = get(&api, &parent.id).await; same_parent(&parent, &resumed);
    let third = resumed.attempt.clone().unwrap(); assert_eq!(third.generation, second.generation + 1);
    assert_eq!(peer.count(), 4); assert_eq!(reported_requests(&api, &parent.id, &third.id).await, 1);
    let resolved = work(&api, &parent.id).await; assert_eq!(resolved.open_waits, 0);
    let WaitStatus::Resolved { resolution, .. } = &resolved.waits.items[0].status else { panic!("wait unresolved") };
    let expected_resolution = QuestionResolution::Answered { question_id:q.id.clone(),
        answer:AnswerRef { question_id:q.id.clone(), answer_id:answer.id.clone() }, selected_option_id:"review".into() };
    assert_eq!(resolution.wait, wait.reference);
    assert_eq!(resolution.outcome, ResolverOutcome::Question(expected_resolution.clone()));
    let activity = api.call(Operation::QueryWorkflowActivity { cursor:None, limit:8 }).await.unwrap();
    let Outcome::Ok { data:Data::WorkflowActivity(page) } = activity.outcome else { panic!("actual terminal activity") };
    assert!(page.next_cursor.is_none());
    let closed = page.items.iter().filter(|event| matches!(&event.detail,
        ActivityDetail::Question(QuestionChange::Closed { resolution }) if resolution == &expected_resolution)).collect::<Vec<_>>();
    assert_eq!(closed.len(), 1); assert_eq!(resolution.event_id, closed[0].id);
    assert_eq!(closed[0].id.product_revision, answer.product_revision);
    let continuation = WorkflowContinuation { task_id:parent.id.clone(), session_id:parent.session_id.clone(),
        resolved_waits:vec![resolution.clone()] };
    let delivery = observe_delivery(&api, &resumed).await;
    let batch = delivery.pending.as_ref().unwrap(); assert_eq!(batch.page.continuation.as_ref(), Some(&continuation));
    assert_eq!(batch.delivery.as_ref().unwrap().context_binding, third.id);
    let before = mirror(&lab, &parent); assert_eq!(before["pending"]["delivery"]["appended"], false);
    let text = before["pending"]["text"].as_str().unwrap();
    let fourth_request = peer.requests.lock().unwrap()[3].clone();
    assert_eq!(fourth_request["input"].as_array().unwrap().iter()
        .filter(|item| item["role"] == "developer" && item["content"] == text).count(), 1);
    let notice: Value = serde_json::from_str(text).unwrap(); assert_eq!(notice["continuation"], json!(continuation));
    assert!(!notice["events"].as_array().unwrap().iter().any(|event| event["id"] == json!(resolution.event_id)),
        "the resolution is represented once by its original continuation, not also duplicated as an event");
    assert!(!fourth_request["input"].as_array().unwrap().iter().any(|item| item["type"] == "function_call_output"));
    answer_release.notify_one(); entered(&review_entered).await;
    let doc = document.lock().unwrap().clone().unwrap(); let rev = review.lock().unwrap().clone().unwrap();
    bound_actor(&doc.version.author, &resumed); bound_actor(&rev.review.requested_by, &resumed);
    assert_eq!(doc.version.content.body, document_body.lock().unwrap().clone().unwrap());
    assert_eq!(rev.review.target, ReviewTarget::DocumentVersion { version:doc.version.reference.clone() });
    let read_doc = api.call(Operation::QueryDocument { query:DocumentQuery::Get {
        document_id:doc.document.id.clone(), version:Some(doc.version.reference.version) } }).await.unwrap();
    assert!(matches!(read_doc.outcome, Outcome::Ok { data:Data::DocumentQuery(DocumentQueryOutcome::Document(ref actual)) } if actual == &doc));
    let read_review = api.call(Operation::QueryReview { query:ReviewQuery::Get { review_id:rev.review.id.clone() } }).await.unwrap();
    assert!(matches!(read_review.outcome, Outcome::Ok { data:Data::ReviewQuery(ref result) } if matches!(result.as_ref(), ReviewQueryOutcome::Review(actual) if actual == &rev)));
    for (path, needle) in [(format!("/documents/{}?version={}", segment(&doc.document.id), doc.version.reference.version), doc.version.reference.content_id.clone()),
        (format!("/reviews/{}", segment(&rev.review.id)), doc.version.reference.content_id.clone()), ("/decisions".into(), rev.review.id.clone())] {
        let response = browser.get(format!("{}{path}", lab.url)).send().await.unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::OK); assert!(response.text().await.unwrap().contains(&needle));
    }
    review_release.notify_one();
    let finished = until(&api, &parent.id, |t| t.status == TaskStatus::Succeeded
        && t.attempt.as_ref().is_some_and(|a| a.stopped)).await;
    same_parent(&parent, &finished); assert_eq!(finished.result.as_ref().unwrap().output, Some(json!(7)));
    assert_eq!(peer.count(), 6);
    assert!(matches!(api.call(Operation::ListTasks { after:None, limit:8 }).await.unwrap().outcome,
        Outcome::Ok { data:Data::Tasks(ref tasks) } if tasks.len() == 2));
    assert!(tokio::time::timeout(Duration::from_millis(150), gate.accept()).await.is_err(), "one coding launch");
    worker.stop().await; controller.stop().await;
    // Read SQLite only after both owners close. Retained originals prove the
    // causal order and receiving attempt; no test-authored worker report or ACK.
    assert_eq!(lab.usage(&first.id).0, 1); assert_eq!(lab.usage(&second.id).0, 2); assert_eq!(lab.usage(&third.id).0, 3);
    let connection = rusqlite::Connection::open_with_flags(lab.root.join("tasks.sqlite"), rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    assert_eq!(connection.query_row("SELECT count(*) FROM tasks", [], |r| r.get::<_, i64>(0)).unwrap(), 2);
    assert_eq!(connection.query_row("SELECT count(*) FROM sessions", [], |r| r.get::<_, i64>(0)).unwrap(), 2);
    assert_eq!(connection.query_row("SELECT count(*) FROM attempts WHERE task_id=?1", [&parent.id], |r| r.get::<_, i64>(0)).unwrap(), 3);
    assert_eq!(connection.query_row("SELECT count(*) FROM attempts WHERE task_id=?1", [&child_id], |r| r.get::<_, i64>(0)).unwrap(), 1);
    let mut command_query = connection.prepare("SELECT body FROM commands WHERE task_id=?1 ORDER BY seq LIMIT 32").unwrap();
    let commands = command_query.query_map([&parent.id], |r| r.get::<_, String>(0)).unwrap()
        .map(|r| serde_json::from_str::<WorkerCommand>(&r.unwrap()).unwrap()).collect::<Vec<_>>();
    assert!(commands.len() < 32);
    let runs = commands.iter().filter_map(|command| match command {
        WorkerCommand::RunAttempt { session, attempt, task, workflow_continuation, .. } => Some((session, attempt, task, workflow_continuation)),
        _ => None,
    }).collect::<Vec<_>>();
    assert_eq!(runs.len(), 3);
    for (index, (session, attempt, input, workflow)) in runs.iter().enumerate() {
        let original_attempt = [&first, &second, &third][index];
        assert_eq!(session.id, parent.session_id); assert_eq!(attempt.id, original_attempt.id);
        let mut expected = parent.input.clone(); expected.limits.model_requests = [7, 5, 3][index];
        assert_eq!(*input, &expected, "only the remaining allowance changes in the Run projection");
        let expected_workflow = if index == 2 { Some(continuation.clone()) } else { None };
        assert_eq!(*workflow, &expected_workflow);
    }
    let journal: Value = serde_json::from_slice(&std::fs::read(lab.state().join("collaboration")
        .join(process::component(&second.id)).join("0000000002.json")).unwrap()).unwrap();
    assert_eq!(journal["consumed"], true);
    let (wait_op, wait_outcome, wait_revision) = receipt(&connection, &wait.operation_id);
    assert_eq!(json!(wait_op), journal["request"]); assert_eq!(json!(wait_outcome), journal["outcome"]);
    let Operation::CallCollaboration { call } = &wait_op else { panic!("original native wait call") };
    assert_eq!(call.tool, "collab_task_wait"); assert_eq!(call.caller.attempt_id, second.id);
    let Outcome::Ok { data: Data::WorkMutation(committed) } = &wait_outcome else { panic!("committed wait") };
    assert_eq!(committed.view.task.attempt.as_ref().unwrap().phase, AttemptPhase::StopRequested);
    assert!(!committed.view.task.attempt.as_ref().unwrap().stopped);
    let (stop_op, stop_outcome, stop_revision) = receipt(&connection, &format!("{}-collaboration-workflow-wait-stopped", second.id));
    let Operation::ObserveWorker { report } = stop_op else { panic!("original physical stop report") };
    assert_eq!(report.attempt_id, second.id); assert_eq!(report.observation, Observation::ProcessTreeStopped);
    assert!(matches!(stop_outcome, Outcome::Ok { .. }));
    let (_, answer_outcome, answer_revision) = receipt(&connection, "journey-browser-answer");
    let Outcome::Ok { data:Data::QuestionMutation(answered) } = answer_outcome else { panic!("original browser answer") };
    assert_eq!(answered.question, answer_view); assert_eq!(answered.new_answer.as_ref(), Some(answer));
    let intent = batch.delivery.as_ref().unwrap();
    let (_, begin_outcome, begin_revision) = receipt(&connection, &intent.operation_id);
    assert!(matches!(begin_outcome, Outcome::Ok { data:Data::CollaborationDelivery(ref begun) }
        if begun.pending.as_ref() == Some(batch)));
    let mut statements = connection.prepare("SELECT request,outcome,controller_revision FROM receipts ORDER BY controller_revision LIMIT 512").unwrap();
    let receipts = statements.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, i64>(2)?))).unwrap()
        .map(|r| { let (request, outcome, revision) = r.unwrap();
            (serde_json::from_str::<Operation>(&request).unwrap(), serde_json::from_str::<Outcome>(&outcome).unwrap(), revision as u64) }).collect::<Vec<_>>();
    assert!(receipts.len() < 512);
    let acknowledgments = receipts.iter().filter(|(op, outcome, _)| matches!(op,
        Operation::AcknowledgeCollaborationDelivery { batch_id, caller, context_binding, .. }
            if batch_id == &batch.batch_id && caller == &intent.caller && context_binding == &third.id)
        && matches!(outcome, Outcome::Ok { .. })).collect::<Vec<_>>();
    assert_eq!(acknowledgments.len(), 1, "one actual native ACK of this occurrence");
    assert_eq!(receipts.iter().filter(|(op, outcome, _)| matches!(op,
        Operation::BeginCollaborationDelivery { batch_id, caller, context_binding, .. }
            if batch_id == &batch.batch_id && caller == &intent.caller && context_binding == &third.id)
        && matches!(outcome, Outcome::Ok { .. })).count(), 1);
    assert!(wait_revision < stop_revision && stop_revision < answer_revision
        && answer_revision < begin_revision && begin_revision < acknowledgments[0].2);
    let native_tools = receipts.iter().filter_map(|(op, outcome, _)| match op {
        Operation::CallCollaboration { call } if call.caller.task_id == parent.id => {
            assert!(matches!(outcome, Outcome::Ok { .. })); Some(call.tool.as_str())
        }, _ => None,
    }).collect::<Vec<_>>();
    assert_eq!(native_tools, vec!["collab_task_delegate", "collab_question_pose", "collab_task_wait", "collab_document_create", "collab_review_create"]);
    let (_, doc_outcome, _) = receipt(&connection, &doc.version.operation_id);
    assert!(matches!(doc_outcome, Outcome::Ok { data:Data::DocumentMutation(DocumentMutationOutcome::Document { ref view }) } if view == &doc));
    let association: Vec<(String, String, Option<String>)> = connection.prepare(
        "SELECT attempt_id,body,input_ack FROM workflow_wait_consumptions WHERE task_id=?1 LIMIT 3").unwrap()
        .query_map([&parent.id], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?))).unwrap().collect::<Result<_, _>>().unwrap();
    assert_eq!(association.len(), 1); assert_eq!(association[0].0, third.id);
    assert_eq!(serde_json::from_str::<Value>(&association[0].1).unwrap(), json!(resolution));
    assert_eq!(serde_json::from_str::<Value>(association[0].2.as_ref().unwrap()).unwrap(),
        json!({"caller":intent.caller,"context_binding":third.id}));
    std::fs::write(lab.root.join("same-parent-journey.json"), serde_json::to_vec_pretty(&json!({
        "accepted_parent":parent,"accepted_child":accepted_child,"after_child":continued,"waiting":waiting,
        "resumed":resumed,"finished":finished,"question":answer_view,"document":doc,"review":rev,
        "resolved_wait":resolution,"delivery_batch":batch,"native_requests":peer.requests.lock().unwrap().clone(),
        "coding_observation":coding_observation,"run_commands":commands,
        "receipt_order":{"wait":wait_revision,"stopped":stop_revision,"answer":answer_revision,
            "begin":begin_revision,"ack":acknowledgments[0].2},
        "scope":"actual native/controller/coding task with controlled Responses and coding protocol peers; no pinned Codex/provider claim"
    })).unwrap()).unwrap();
    drop(statements); drop(command_query);
    drop(connection);
    let mut reopened = lab.controller("controller-reopened").await;
    assert_eq!(get(&api, &parent.id).await, finished); assert_eq!(get(&api, &child_id).await, accepted_child);
    assert_eq!(question(&api, &q.id).await, answer_view); reopened.stop().await;
}
