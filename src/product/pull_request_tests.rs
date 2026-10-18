use super::*;
use crate::{pull_request as pr, pull_request_view as public};
use serde_json::json;

fn target() -> pr::Target {
    pr::Target { revision: "target-one".into(), api_base: "https://api.github.com".into(),
        owner: "fixture".into(), repository: "project".into(), repository_id: 7,
        head_remote: "https://github.com/private-route/project.git".into(),
        base_branch: "main".into(), credential_name: "GH_TOKEN".into() }
}
fn fields() -> Form {
    let metadata = RuntimeLimits::seed().unwrap();
    let mut fields = create_defaults(ExecutorKind::Coding, &metadata).unwrap();
    for (key, value) in [("operation_id", "pr-submit"), ("instruction", "Work"),
        ("definition_id", "pr-agent"), ("definition_revision", "one"),
        ("repository_id", "pr-repository"), ("registration_revision", "one"),
        ("start_commit", "1111111111111111111111111111111111111111")] {
        fields.insert(key.into(), value.into());
    }
    fields
}
fn sample() -> Task {
    let mut input = task_input(&fields(), &RuntimeLimits::seed().unwrap()).unwrap();
    input.pull_request = Some(pr::Request { title: "Explicit <title>".into(), body: "Keep user text: credential_name".into() });
    input.repository.as_mut().unwrap().pull_request_target = Some(target());
    Task { id: "pr-task".into(), session_id: "pr-session".into(), assigned_agent_id: input.definition.id.clone(),
        input, status: TaskStatus::Queued, revision: 1, attempt: None, selected_checkpoint: None,
        result: None, blocked_reason: None, pending_assignment: None, native_retry_block: None, progress: String::new(),
        recent_output: String::new(), output_truncated: false, parent_task_id: None,
        waiting_child_id: None, completed_child: None }
}

#[test]
fn explicit_request_forms_preserve_body_and_refuse_target_or_native_publication() {
    let metadata = RuntimeLimits::seed().unwrap();
    let mut values = fields();
    assert!(task_input(&values, &metadata).unwrap().pull_request.is_none());
    values.insert("pull_request_title".into(), "  Exact title  ".into());
    values.insert("pull_request_body".into(), "line one\nline two\n".into());
    let input = task_input(&values, &metadata).unwrap();
    assert_eq!(input.pull_request, Some(pr::Request { title: "  Exact title  ".into(), body: "line one\nline two\n".into() }));
    assert!(input.repository.unwrap().pull_request_target.is_none());
    values.insert("pull_request_target".into(), serde_json::to_string(&target()).unwrap());
    assert!(task_input(&values, &metadata).is_err());
    values.remove("pull_request_target"); values.remove("pull_request_title");
    assert!(task_input(&values, &metadata).is_err());
    values.insert("pull_request_title".into(), "Exact title".into());
    values.insert("executor".into(), "native".into());
    for key in ["repository_id", "registration_revision", "start_commit"] { values.remove(key); }
    values.insert("acceptance_commands".into(), "[]".into());
    values.insert("output_schema".into(), "{\"type\":\"integer\"}".into());
    assert!(task_input(&values, &metadata).is_err());
}

#[test]
fn configured_child_request_is_explicit_and_cannot_supply_or_redisplay_private_target() {
    let metadata = RuntimeLimits::seed().unwrap();
    let mut task = sample();
    task.input.repository.as_mut().unwrap().pull_request_target = None;
    let child = CodingTaskTemplate { instruction_source: None, pull_request: task.input.pull_request.clone(), instruction_prefix: "Child".into(),
        definition: task.input.definition.clone(), repository: task.input.repository.clone().unwrap(),
        acceptance_commands: task.input.acceptance_commands.clone(), dependencies: vec![], limits: task.input.limits.clone() };
    let mut value = serde_json::to_value(NativeWorkflow::CodingThenContinue { child }).unwrap();
    let encoded = value.to_string();
    let decoded = task_defaults::native_workflow(&encoded, task.input.limits.model_requests, false, &metadata).unwrap().unwrap();
    let NativeWorkflow::CodingThenContinue { child } = decoded.as_ref() else { panic!("configured child missing"); };
    assert_eq!(child.pull_request, task.input.pull_request);
    assert!(task_defaults::may_redisplay_native_workflow(&encoded));
    value["child"]["repository"]["pull_request_target"] = serde_json::to_value(target()).unwrap();
    assert!(task_defaults::native_workflow(&value.to_string(), task.input.limits.model_requests, false, &metadata).is_err());
    assert!(!task_defaults::may_redisplay_native_workflow(&value.to_string()));
}

#[test]
fn public_task_copies_preserve_requests_and_user_json_without_changing_canonical_input() {
    let mut original = sample();
    original.input.output_schema = Some(json!({"credential_name":"user-owned","head_remote":"user-owned"}));
    let child = CodingTaskTemplate { instruction_source: None, pull_request: original.input.pull_request.clone(), instruction_prefix: "Child".into(),
        definition: original.input.definition.clone(), repository: original.input.repository.clone().unwrap(),
        acceptance_commands: vec![], dependencies: vec![], limits: original.input.limits.clone() };
    original.input.native_workflow = Some(Box::new(NativeWorkflow::CodingThenContinue { child }));
    let before = original.clone();
    let projected = public::task(&original);
    assert!(projected.input.repository.as_ref().unwrap().pull_request_target.is_none());
    let NativeWorkflow::CodingThenContinue { child } = projected.input.native_workflow.as_deref().unwrap() else { panic!(); };
    assert!(child.repository.pull_request_target.is_none());
    assert_eq!(projected.input.pull_request, original.input.pull_request);
    assert_eq!(projected.input.output_schema, original.input.output_schema);
    assert_eq!(original, before);
    let body = render_task(&original, false).body;
    assert!(body.contains("Explicit &lt;title&gt;") && body.contains("Keep user text: credential_name"));
    for private in [target().api_base, target().head_remote, target().credential_name] { assert!(!body.contains(&private)); }
}

fn branch(commit: &str) -> pr::BranchProof {
    pr::BranchProof { repository_id: 7, owner: "fixture".into(), repository: "project".into(), branch: "branch".into(), commit: commit.into() }
}
#[test]
fn accepted_pr_links_keep_exact_original_references_and_never_execute_unsafe_urls() {
    let mut reference = pr::ResultReference { operation_id: "pr-original".into(), head_operation_id: "head-original".into(),
        checkpoint_operation_id: "checkpoint-original".into(), target_revision: "target-one".into(), provider_id: 9,
        number: 3, url: "https://github.com/fixture/project/pull/3".into(), head: branch(&"a".repeat(40)), base: branch(&"b".repeat(40)) };
    let body = pull_request_result_view(&reference);
    for identity in [&reference.operation_id, &reference.head_operation_id, &reference.checkpoint_operation_id, &reference.head.commit, &reference.base.commit] { assert!(body.contains(identity)); }
    assert!(body.contains("href=\"https://github.com/fixture/project/pull/3\""));
    reference.url = "javascript:alert('private')".into();
    let unsafe_view = pull_request_result_view(&reference);
    assert!(!unsafe_view.contains("href=\"javascript:"));
    assert!(unsafe_view.contains("javascript:alert"));
}

#[test]
fn pr_operation_view_uses_typed_original_identity_and_omits_private_target_and_head_route() {
    let binding = pr::Binding { operation_id: "pr-original".into(), task_id: "task".into(), attempt_id: "attempt".into(), generation: 1,
        registration: crate::repository_catalogue::Revision { repository_id: "repo".into(), registration_revision: "one".into() },
        checkpoint: Checkpoint { registration: crate::repository_catalogue::Revision { repository_id: "repo".into(), registration_revision: "one".into() },
            repository: "public-checkpoint".into(), commit: "a".repeat(40), remote_ref: "refs/checkpoints/one".into(), operation_id: "checkpoint-original".into() } };
    let intent = pr::HeadIntent { binding, publication: None, target: target(), head_operation_id: "head-original".into(), head_ref: "refs/heads/pr-one".into() };
    let proof = pr::HeadProof { operation_id: "head-original".into(), target_revision: "target-one".into(), remote: target().head_remote,
        remote_ref: intent.head_ref.clone(), commit: "a".repeat(40) };
    let record = OperationRecord { id: "head-original".into(), task_id: "task".into(), attempt_id: "attempt".into(), generation: 1,
        kind: pr::HEAD_KIND.into(), intended: serde_json::to_value(intent).unwrap(), state: "settled".into(),
        outcome: Some(serde_json::to_value(pr::HeadOutcome::Applied(pr::HeadSettlement { acknowledged: true, proof })).unwrap()) };
    let before = record.clone();
    let view = public::record(&record).unwrap();
    assert_eq!(view.id, record.id); assert_eq!(view.state, record.state);
    assert_eq!(view.intended["head_ref"], "refs/heads/pr-one");
    assert_eq!(view.outcome.as_ref().unwrap()["proof"]["commit"], "a".repeat(40));
    let encoded = serde_json::to_string(&view).unwrap();
    for private in [target().api_base, target().head_remote, target().credential_name] { assert!(!encoded.contains(&private)); }
    assert_eq!(record, before);
}

async fn actual(controller: &Controller, command: Operation) -> Reply {
    Box::pin(controller.handle(crate::protocol::Request { protocol_revision: WIRE_REVISION,
        request_id: identity(), command })).await
}
fn task_reply(reply: Reply) -> Task {
    let Outcome::Ok { data: Data::Task(task) } = reply.outcome else { panic!("expected actual task: {reply:?}"); };
    *task
}

#[tokio::test]
async fn actual_ui_and_mcp_keep_private_target_canonical_and_original_public_receipts_exact() {
    let root = std::env::temp_dir().join(format!("borg-pr-public-view-{}", identity()));
    std::fs::create_dir(&root).unwrap();
    let cfg = crate::controller::ControllerConfig::default();
    let controller = Controller::open_with_config(root.join("tasks.sqlite"), cfg.clone()).unwrap();
    let mut registration = Form::from([
        ("operation_id".into(), "pr-register".into()), ("repository_id".into(), "pr-repository".into()),
        ("registration_revision".into(), "one".into()), ("display_name".into(), "PR fixture".into()),
        ("source_endpoint".into(), "/fixture/source.git".into()), ("checkpoint_endpoint".into(), "/fixture/checkpoint.git".into()),
        ("source_store_key".into(), "source.git".into()), ("checkpoint_store_key".into(), "checkpoint.git".into()),
        ("pull_request_target".into(), serde_json::to_string(&target()).unwrap()),
    ]);
    let response = Box::pin(post(&controller, "/repositories", registration.clone())).await;
    assert_eq!(response.status, 303, "{}", response.body);
    for private in [target().api_base, target().head_remote, target().credential_name] { assert!(!response.body.contains(&private)); }
    registration.insert("unknown".into(), "rejected".into());
    let refused = Box::pin(post(&controller, "/repositories", registration)).await;
    assert_eq!(refused.status, 400);
    for private in [target().api_base, target().head_remote, target().credential_name] { assert!(!refused.body.contains(&private)); }
    let mut values = fields();
    values.insert("pull_request_title".into(), "Explicit PR".into());
    values.insert("pull_request_body".into(), "Exact request body".into());
    let input = task_input(&values, controller.limits()).unwrap();
    let submitted = task_reply(actual(&controller, Operation::SubmitTask { operation_id: "pr-submit".into(), input }).await);
    assert_eq!(submitted.input.repository.as_ref().unwrap().pull_request_target, Some(target()));
    let accepted = submitted.input.clone();
    let registered = actual(&controller, Operation::RegisterWorker { operation_id: "pr-worker-register".into(),
        worker: Worker { id: "pr-worker".into(), incarnation: "one".into(), coding_capacity: 1, native_capacity: 0 } }).await;
    assert!(matches!(registered.outcome, Outcome::Ok { data: Data::Worker(_) }));
    let commands = actual(&controller, Operation::AwaitCommands { operation_id: "pr-run".into(), worker_id: "pr-worker".into(), incarnation: "one".into(), acknowledged: vec![] }).await;
    let Outcome::Ok { data: Data::Commands(commands) } = commands.outcome else { panic!("real run command missing"); };
    let WorkerCommand::RunAttempt { attempt, task: command_input, .. } = commands.into_iter().find(|command| matches!(command, WorkerCommand::RunAttempt { .. })).unwrap() else { unreachable!() };
    assert_eq!(command_input, accepted);
    let started = actual(&controller, Operation::ObserveWorker { report: Report { report_id: "pr-started".into(),
        worker_id: attempt.worker_id.clone(), incarnation: attempt.worker_incarnation.clone(), task_id: submitted.id.clone(),
        attempt_id: attempt.id.clone(), generation: attempt.generation,
        observation: Observation::Started { thread_id: Some("thread".into()), turn_id: Some("turn".into()) } } }).await;
    assert!(matches!(started.outcome, Outcome::Ok { data: Data::Task(_) }));
    let caller = crate::mcp::Caller { task_id: submitted.id.clone(), attempt_id: attempt.id.clone(), generation: attempt.generation,
        worker_id: attempt.worker_id, incarnation: attempt.worker_incarnation };
    let invocation = crate::mcp::Invocation { operation_id: "pr-public-get".into(), logical_call_id: "pr-public-logical".into(),
        caller: caller.clone(), tool: "collab_task_get".into(), arguments: json!({"task_id":submitted.id}) };
    let original = actual(&controller, invocation.operation()).await;
    let projected = task_reply(original.clone());
    assert!(projected.input.repository.as_ref().unwrap().pull_request_target.is_none());
    assert_eq!(projected.input.pull_request, accepted.pull_request);
    assert_eq!(task_reply(actual(&controller, Operation::GetTask { task_id: submitted.id.clone() }).await).input, accepted);
    let replay = actual(&controller, invocation.operation()).await;
    assert_eq!(replay.outcome, original.outcome); assert_eq!(replay.commit, original.commit);
    let lookup = actual(&controller, Operation::GetCollaborationCall { caller, operation_id: invocation.operation_id.clone() }).await;
    let recovered = crate::mcp::recovered(&invocation, lookup).unwrap();
    assert_eq!(recovered.reply.outcome, original.outcome); assert_eq!(recovered.reply.commit, original.commit);
    let catalogue = actual(&controller, Operation::GetRepository { repository_id: "pr-repository".into(), registration_revision: Some("one".into()) }).await;
    assert!(matches!(catalogue.outcome, Outcome::Ok { data: Data::RepositoryRegistration(entry) } if entry.pull_request_available));
    let encoded = serde_json::to_string(&original.outcome).unwrap();
    for private in [target().api_base, target().head_remote, target().credential_name] { assert!(!encoded.contains(&private)); }
    drop(controller);
    // No SQL probes or physical-process claim: this test exercises actual
    // controller/UI/MCP operations; the separate recovery cases own workers.
}
