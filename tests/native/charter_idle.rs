//! Nested under charter_worker::callers. Actual local Controller/native worker;
//! no external provider or executed Coding child. Existing process/profile helpers.
use super::*;
use std::collections::BTreeMap;

pub(super) async fn attempts(
    api: &ApiClient,
    task: &str,
) -> Vec<borg_runtime::collaboration::AttemptView> {
    let operation = Operation::QueryCollaboration {
        query: borg_runtime::collaboration::Query::TaskAttempts {
            task_id: task.into(),
            after: None,
            limit: 8,
        },
    };
    let reply = api.call(operation.clone()).await.unwrap();
    reply.validate_for(&operation).unwrap();
    let Outcome::Ok {
        data: Data::Collaboration(borg_runtime::collaboration::QueryResult::TaskAttempts(page)),
    } = reply.outcome
    else {
        panic!("canonical attempt page: {reply:?}")
    };
    assert!(page.next_cursor.is_none());
    page.items
}
async fn all_tasks(api: &ApiClient) -> Vec<Task> {
    let reply = api
        .call(Operation::ListTasks {
            after: None,
            limit: 8,
        })
        .await
        .unwrap();
    let Outcome::Ok {
        data: Data::Tasks(tasks),
    } = reply.outcome
    else {
        panic!("tasks: {reply:?}")
    };
    assert!(tasks.len() < 8, "fixture task inventory is complete");
    tasks
}
fn runs(lab: &Lab) -> BTreeMap<String, Value> {
    let mut result = BTreeMap::new();
    for entry in std::fs::read_dir(lab.state().join("commands")).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(|v| v.to_str()) != Some("json") {
            continue;
        }
        let value: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        let command: WorkerCommand = serde_json::from_value(value["command"].clone()).unwrap();
        if let WorkerCommand::RunAttempt { command_id, .. } = command {
            assert!(result.insert(command_id, value).is_none());
        }
    }
    result
}
pub(super) async fn settled_run(lab: &Lab, attempt: &Attempt, worker: &mut Process) {
    let until = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        assert!(
            worker.child.try_wait().unwrap().is_none(),
            "actual worker exited"
        );
        if runs(lab)
            .get(&format!("run-{}", attempt.id))
            .is_some_and(|v| v["completed"] == true)
        {
            completed_command(lab, &attempt.id);
            return;
        }
        assert!(
            tokio::time::Instant::now() < until,
            "original RunAttempt not completed"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
fn awaited(proxy: &Proxy, incarnation: &str) -> usize {
    proxy
        .await_replies
        .lock()
        .unwrap()
        .iter()
        .filter(|(worker, inc)| worker == "loop-worker" && inc == incarnation)
        .count()
}
async fn await_cycles(
    lab: &Lab,
    proxy: &Proxy,
    incarnation: &str,
    before: usize,
    worker: &mut Process,
) {
    // Two replies ensure one complete request/response cycle began after the
    // update, even when the first AwaitCommands was already in flight.
    let bound = Duration::from_millis(lab.limits.controller.command_wait_ms)
        .checked_add(Duration::from_millis(
            lab.limits.transport.client_timeout_ms,
        ))
        .unwrap()
        .checked_mul(2)
        .unwrap();
    tokio::time::timeout(bound, async {
        while awaited(proxy, incarnation) < before + 2 {
            assert!(
                worker.child.try_wait().unwrap().is_none(),
                "idle consumer exited"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("post-update actual AwaitCommands cycle absent");
}
async fn retain_source(api: &ApiClient, lab: &Lab) {
    let mut value = input(lab, 1);
    value.definition.revision = "r2".into();
    value.definition.charter = Some(source("r2"));
    let retained = submit_identified(api, value, "idle-retain-r2").await;
    assert!(matches!(
        api.call(Operation::CancelTask {
            operation_id: "idle-cancel-source-task".into(),
            task_id: retained.id,
            expected_revision: retained.revision,
        })
        .await
        .unwrap()
        .outcome,
        Outcome::Ok { .. }
    ));
}

//= spec/impl/v2/implementation.md#charter-changes-apply-at-safe-boundaries
//# A charter change MUST NOT wake an idle session solely to adopt it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pending_charter_keeps_idle_parent_quiescent_until_independent_child_resolution() {
    Box::pin(idle_parent()).await;
}
async fn idle_parent() {
    let mut lab = Lab::new("charter-idle-pending-child");
    configure(&mut lab);
    assert_eq!(lab.limits.worker.coding_capacity, 0);
    let mut controller = lab.controller("idle-controller").await;
    let api = ApiClient::new(&lab.url, lab.limits.clone()).unwrap();
    retain_source(&api, &lab).await;
    // Same admitted, deliberately unopened repository fixture used by the
    // existing configured-child quiescence test. No Coding attempt executes.
    let registration = borg_runtime::repository_catalogue::Registration {
        pull_request_target: None,
        repository_id: "idle-child-repository".into(),
        registration_revision: "1".into(),
        display_name: "Idle child fixture".into(),
        source_endpoint: lab
            .root
            .join("unopened-repository.git")
            .display()
            .to_string(),
        checkpoint_endpoint: lab
            .root
            .join("unopened-repository.git")
            .display()
            .to_string(),
        source_store_key: "unopened-repository.git".into(),
        checkpoint_store_key: "unopened-repository.git".into(),
    };
    assert!(matches!(
        api.call(Operation::RegisterRepository {
            operation_id: "idle-register-child-source".into(),
            registration: registration.clone(),
        })
        .await
        .unwrap()
        .outcome,
        Outcome::Ok {
            data: Data::RepositoryRegistration(_)
        }
    ));
    let mut accepted = input(&lab, 3);
    let mut child_limits = accepted.limits.clone();
    child_limits.model_requests = 1;
    child_limits.result_bytes = 8192;
    child_limits.max_delegation_depth = 0;
    accepted.native_workflow = Some(Box::new(NativeWorkflow::CodingThenContinue {
        child: CodingTaskTemplate {
            instruction_source: None,
            pull_request: None,
            instruction_prefix: "Bounded idle child".into(),
            definition: Definition {
                charter: Some(source("r1")),
                limit_overrides: None,
                inference: Some(InferenceSettings::selected()),
                execution_profile: Some(ExecutionProfile::CollaborationV1 { web_search: None }),
                id: "idle-coding-child".into(),
                revision: "1".into(),
                executor: ExecutorKind::Coding,
            },
            repository: Repository {
                pull_request_target: None,
                registration: Some(registration.binding()),
                url: String::new(),
                checkpoint_remote: String::new(),
                start_commit: "a".repeat(40),
            },
            acceptance_commands: vec![],
            dependencies: vec![],
            limits: child_limits,
        },
    }));
    let task = submit(&api, accepted).await;
    let resumed_entered = Arc::new(tokio::sync::Notify::new());
    let resumed_release = Arc::new(tokio::sync::Notify::new());
    let peer = Peer::start_async({
        let entered = resumed_entered.clone();
        let release = resumed_release.clone();
        move |index, request| {
            let entered = entered.clone();
            let release = release.clone();
            async move {
                assert!(request.get("tools").is_some());
                match index {
                    0 => {
                        assert!(request["input"].to_string().contains("r1"));
                        let mut reply = tool_reply(0, "r1");
                        reply["output"][1]["name"] = json!("collab_task_delegate");
                        reply["output"][1]["arguments"] = json!(
                            json!({"instruction":"Perform the configured child work."}).to_string()
                        );
                        Some(reply)
                    }
                    1 => {
                        assert!(request["input"].to_string().contains("r2"));
                        entered.notify_one();
                        release.notified().await;
                        Some(final_reply("r2", json!(7)))
                    }
                    _ => panic!("pending update or resolved child caused an extra POST"),
                }
            }
        }
    })
    .await;
    let proxy = Proxy::start(lab.url.clone(), None, false, lab.limits.clone()).await;
    let incarnation = "idle-native-worker";
    let mut worker = lab.worker(&proxy.url, &peer.endpoint, incarnation);
    let waiting = until(&api, &task.id, |t| {
        t.waiting_child_id.is_some()
            && t.attempt
                .as_ref()
                .is_some_and(|a| a.stopped && !a.uncertain)
    })
    .await;
    let first = waiting.attempt.as_ref().unwrap().clone();
    settled_run(&lab, &first, &mut worker).await;
    let waiting = get(&api, &task.id).await;
    assert_eq!(waiting.input, task.input);
    assert_eq!(waiting.session_id, task.session_id);
    assert!(waiting.result.is_none());
    let child_id = waiting.waiting_child_id.clone().unwrap();
    let child = get(&api, &child_id).await;
    assert_eq!(child.status, TaskStatus::Queued);
    assert!(child.attempt.is_none());
    assert_eq!(child.parent_task_id.as_deref(), Some(task.id.as_str()));
    let tasks_before = all_tasks(&api).await;
    assert_eq!(tasks_before.len(), 3);
    let attempts_before = attempts(&api, &task.id).await;
    assert_eq!(attempts_before.len(), 1);
    assert_eq!(attempts_before[0].reported_model_requests, 1);
    let runs_before = runs(&lab);
    assert_eq!(runs_before.len(), 1);
    let turns_before = turns(&api, &task.session_id).await;
    assert_eq!(turns_before.len(), 1);
    assert!(turns_before[0].reply.is_some() && turns_before[0].completed_by.is_none());
    let before = binding(&api, &task.session_id).await;
    assert_eq!(before.session.effective, reference("r1"));
    assert!(before.session.pending.is_none());
    let changed = pending(&api, &task.session_id, "r2").await;
    assert!(matches!(changed.outcome, Outcome::Ok { .. }));
    let change_revision = changed.commit.as_ref().unwrap().controller_revision;
    let pending_binding = binding(&api, &task.session_id).await;
    assert_eq!(pending_binding.session.effective, reference("r1"));
    assert_eq!(pending_binding.session.pending, Some(reference("r2")));
    assert_eq!(
        pending_binding.session.revision,
        before.session.revision + 1
    );
    let before_await = awaited(&proxy, incarnation);
    await_cycles(&lab, &proxy, incarnation, before_await, &mut worker).await;
    assert_eq!(
        all_tasks(&api).await,
        tasks_before,
        "SetPending created or changed task work"
    );
    assert_eq!(attempts(&api, &task.id).await, attempts_before);
    assert_eq!(runs(&lab), runs_before);
    assert_eq!(turns(&api, &task.session_id).await, turns_before);
    assert_eq!(binding(&api, &task.session_id).await, pending_binding);
    assert_eq!(peer.count(), 1);
    assert!(process::now_ms() < first.deadline_unix_ms);
    // This independent operator resolution, not SetPending, makes the parent
    // continuation eligible and delivers the original terminal child outcome.
    let resolution = api
        .call(Operation::CancelTask {
            operation_id: "idle-independent-child-resolution".into(),
            task_id: child_id.clone(),
            expected_revision: child.revision,
        })
        .await
        .unwrap();
    assert!(matches!(
        resolution.outcome,
        Outcome::Ok {
            data: Data::Task(_)
        }
    ));
    assert!(resolution.commit.as_ref().unwrap().controller_revision > change_revision);
    tokio::time::timeout(Duration::from_secs(30), resumed_entered.notified())
        .await
        .unwrap();
    let resumed = get(&api, &task.id).await;
    let fresh = resumed.attempt.as_ref().unwrap().clone();
    assert_eq!(resumed.status, TaskStatus::Active);
    assert_eq!(resumed.input, task.input);
    assert_eq!(resumed.session_id, task.session_id);
    assert_eq!(resumed.assigned_agent_id, task.assigned_agent_id);
    assert_ne!(fresh.id, first.id);
    assert_eq!(fresh.generation, first.generation + 1);
    assert_eq!(fresh.worker_incarnation, incarnation);
    assert_eq!(fresh.deadline_unix_ms, first.deadline_unix_ms);
    assert!(!fresh.stopped && !fresh.uncertain);
    let outcome = resumed.completed_child.as_ref().unwrap();
    assert_eq!(outcome.task_id, child_id);
    assert_eq!(outcome.status, TaskStatus::Cancelled);
    let adopted = binding(&api, &task.session_id).await;
    assert_eq!(adopted.session.effective, reference("r2"));
    assert!(adopted.session.pending.is_none());
    let ordinary = turns(&api, &task.session_id).await;
    assert_eq!(ordinary.len(), 2);
    let old = ordinary
        .iter()
        .find(|v| v.boundary.caller.attempt_id == first.id)
        .unwrap();
    assert_eq!(old.boundary, turns_before[0].boundary);
    assert_eq!(old.reply, turns_before[0].reply);
    assert!(
        old.completed_by.is_some(),
        "exact child result closes the old turn"
    );
    let current = ordinary
        .iter()
        .find(|v| v.boundary.caller.attempt_id == fresh.id)
        .unwrap();
    assert_eq!(current.boundary.request_index, 1);
    assert_eq!(current.boundary.effective.reference, reference("r2"));
    assert!(current.reply.is_none(), "first resumed POST remains held");
    let body = peer.requests.lock().unwrap()[1].clone();
    let continuation =
        borg_runtime::delegation::continuation_instruction(&task.input, outcome).unwrap();
    assert_eq!(body["input"][0]["content"], continuation);
    assert_eq!(peer.count(), 2);
    resumed_release.notify_one();
    let done = until(&api, &task.id, |t| {
        t.status == TaskStatus::Succeeded && t.attempt.as_ref().is_some_and(|a| a.stopped)
    })
    .await;
    assert_eq!(done.result.as_ref().unwrap().output, Some(json!(7)));
    assert_eq!(done.input, task.input);
    let charged = attempts(&api, &task.id).await;
    assert_eq!(charged.len(), 2);
    assert!(charged.iter().all(|a| a.reported_model_requests == 1));
    assert_eq!(peer.count(), 2);
    assert!(attempts(&api, &child_id).await.is_empty());
    let final_binding = binding(&api, &task.session_id).await;
    let final_turns = turns(&api, &task.session_id).await;
    std::fs::write(lab.root.join("idle-pending-proof.json"), serde_json::to_vec_pretty(&json!({
        "original_task":task,"idle_task":waiting,"idle_child":child,"tasks_before":tasks_before,
        "attempts_before":attempts_before,"runs_before":runs_before,"pending":pending_binding,
        "pending_receipt":changed,"await_before":before_await,"await_after":awaited(&proxy,incarnation),
        "independent_resolution":resolution,"first_resumed_post":body,"resumed_task":resumed,
        "terminal_task":done,"final_attempts":charged,"final_binding":final_binding,
    })).unwrap()).unwrap();
    worker.stop().await;
    controller.stop().await;
    let mut controller = lab.controller("idle-controller-reopen").await;
    assert_eq!(get(&api, &task.id).await, done);
    assert_eq!(binding(&api, &task.session_id).await, final_binding);
    assert_eq!(turns(&api, &task.session_id).await, final_turns);
    assert_eq!(peer.count(), 2);
    controller.stop().await;
}
