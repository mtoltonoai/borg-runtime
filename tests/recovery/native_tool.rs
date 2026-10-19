//! Controlled provider transport plus the actual native worker/controller. No
//! provider model or Codex process is invoked by these local integration cases.
use super::*;
use borg_runtime::native_loop_contract as contract;
use tokio::sync::oneshot;

struct Peer {
    endpoint: String,
    requests: Arc<Mutex<Vec<Value>>>,
    events: mpsc::Receiver<usize>,
    release: Option<oneshot::Sender<()>>,
    join: tokio::task::JoinHandle<()>,
}
impl Peer {
    async fn start(
        partial_usage: bool,
        hold_first: bool,
        task_id: String,
        limits: Arc<RuntimeLimits>,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/v1/responses", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let observed = requests.clone();
        let (send, events) = mpsc::channel(4);
        let (release, mut held) = oneshot::channel();
        let join = tokio::spawn(async move {
            for step in 0..2 {
                let (socket, _) = listener.accept().await.unwrap();
                let mut stream = BufReader::new(socket);
                let mut head = String::new();
                loop {
                    let mut line = String::new();
                    assert!(stream.read_line(&mut line).await.unwrap() > 0);
                    head.push_str(&line);
                    assert!(head.len() < 16 * 1024);
                    if line == "\r\n" {
                        break;
                    }
                }
                assert!(head.starts_with("POST /v1/responses HTTP/1.1\r\n"));
                let len: usize = head
                    .lines()
                    .find_map(|line| {
                        let (key, value) = line.split_once(':')?;
                        key.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse().unwrap())
                    })
                    .unwrap();
                assert!(len <= limits.execution.native.request_bytes);
                let mut body = vec![0; len];
                stream.read_exact(&mut body).await.unwrap();
                let request: Value = serde_json::from_slice(&body).unwrap();
                assert_eq!(request["store"], false);
                assert!(request.get("previous_response_id").is_none());
                assert_eq!(request["model"], "gpt-6-astra");
                assert_eq!(request["reasoning"], serde_json::json!({"effort":"max"}));
                let output = if step == 0 {
                    assert_eq!(request["tool_choice"], "auto");
                    assert_eq!(request["parallel_tool_calls"], false);
                    let selected = request["tools"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .find(|t| t["name"] == "collab_task_get")
                        .unwrap();
                    assert_eq!(
                        selected["parameters"],
                        json!({"type":"object","properties":{"task_id":{"type":"string","minLength":1,"maxLength":limits.transport.identity_bytes,"x-borg-maxUtf8Bytes":limits.transport.identity_bytes}},"required":["task_id"],"additionalProperties":false})
                    );
                    assert_eq!(
                        request["tools"].as_array().unwrap().len(),
                        borg_runtime::mcp::catalogue(&limits).len()
                    );
                    json!([
                        {"type":"reasoning","id":"rs_fixture","summary":[],"encrypted_content":"synthetic-opaque-context"},
                        {"type":"message","id":"msg_fixture","role":"assistant","status":"completed","content":[{"type":"output_text","text":"I will inspect this task.","annotations":[]}]},
                        {"type":"function_call","id":"fc_fixture","call_id":"call_current_task","name":"collab_task_get","arguments":json!({"task_id":task_id}).to_string(),"status":"completed"}
                    ])
                } else {
                    assert_eq!(request["tool_choice"], "auto");
                    assert_eq!(request["parallel_tool_calls"], false);
                    let items = request["input"].as_array().unwrap();
                    assert_eq!(items.len(), 5);
                    assert_eq!(items[1]["encrypted_content"], "synthetic-opaque-context");
                    assert_eq!(items[2]["content"][0]["text"], "I will inspect this task.");
                    assert_eq!(items[3]["call_id"], "call_current_task");
                    assert_eq!(items[4]["type"], "function_call_output");
                    assert_eq!(items[4]["call_id"], items[3]["call_id"]);
                    let actual: Value =
                        serde_json::from_str(items[4]["output"].as_str().unwrap()).unwrap();
                    assert_eq!(actual["logical_call_id"], "call_current_task");
                    let outcome: Outcome =
                        serde_json::from_value(actual["outcome"].clone()).unwrap();
                    let Outcome::Ok {
                        data: Data::Task(actual_task),
                    } = outcome
                    else {
                        panic!("tool result must be actual task")
                    };
                    assert_eq!(actual_task.id, task_id);
                    assert_eq!(actual_task.status, TaskStatus::Active);
                    let attempt = actual_task.attempt.as_ref().unwrap();
                    assert_eq!(actual["operation_id"], contract::call_id(&attempt.id, 1));
                    let commit: MutationCommit =
                        serde_json::from_value(actual["commit"].clone()).unwrap();
                    assert!(commit.controller_revision > 0);
                    // Final output derives from the actual committed MCP task read.
                    let result = json!({"task_id":actual_task.id,"agent_id":actual_task.assigned_agent_id,"attempt_id":attempt.id,"generation":attempt.generation});
                    json!([{"type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":result.to_string()}]}])
                };
                observed.lock().unwrap().push(request);
                send.send(step + 1).await.unwrap();
                if step == 0 && hold_first {
                    let _ = (&mut held).await;
                }
                let mut envelope = json!({"status":"completed","output":output});
                if step == 0 || !partial_usage {
                    envelope["usage"] =
                        json!({"input_tokens":17,"output_tokens":9,"total_tokens":26});
                }
                let response = serde_json::to_vec(&envelope).unwrap();
                if stream.get_mut().write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",response.len()).as_bytes()).await.is_err() { return; }
                if stream.get_mut().write_all(&response).await.is_err() {
                    return;
                }
                let _ = stream.get_mut().flush().await;
            }
        });
        Self {
            endpoint,
            requests,
            events,
            release: Some(release),
            join,
        }
    }
    async fn event(&mut self) -> usize {
        tokio::time::timeout(TIMEOUT, self.events.recv())
            .await
            .unwrap()
            .unwrap()
    }
}
impl Drop for Peer {
    fn drop(&mut self) {
        self.join.abort();
    }
}
fn input(deadline: u64, limits: &RuntimeLimits) -> TaskInput {
    TaskInput { instruction_source: None,
        pull_request: None,
        resolved_limits: None,
        instruction:"Inspect your current task using its configured tool, then return its task_id, agent_id, attempt_id and generation as JSON.".into(),
        definition:selected_definition("native-task-inspector",ExecutorKind::Native),repository:None,repository_read:None,acceptance_commands:vec![],dependencies:vec![],
        output_schema:Some(json!({"type":"object","required":["task_id","agent_id","attempt_id","generation"],"properties":{"task_id":{"type":"string"},"agent_id":{"type":"string"},"attempt_id":{"type":"string"},"generation":{"type":"integer"}},"additionalProperties":false})),
        limits:Limits{deadline_unix_ms:deadline,model_requests:2,output_bytes:4096,result_bytes:limits.task_defaults.result_bytes,max_delegation_depth:0},
        native_workflow:None,
    }
}
async fn submit_tool(lab: &Lab, deadline: u64) -> Task {
    task(
        lab.client
            .call(Operation::SubmitTask {
                operation_id: "submit-native-tool".into(),
                input: (input(deadline, &lab.limits)).into(),
            })
            .await
            .unwrap(),
    )
}
async fn receipt(lab: &Lab, id: &str) -> (Reply, OperationRecord) {
    let reply = lab
        .client
        .call(Operation::GetOperation {
            operation_id: id.into(),
        })
        .await
        .unwrap();
    let Outcome::Ok {
        data: Data::Operation(record),
    } = &reply.outcome
    else {
        panic!("missing receipt {reply:?}")
    };
    (reply.clone(), record.clone())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
//= spec/impl/v2/implementation.md#the-native-executor-is-bounded
//= type=test
//# The native executor MUST support a bounded direct tool invocation followed by a model continuation.
//= spec/impl/v2/implementation.md#the-native-executor-is-bounded
//= type=test
//# A native model continuation MUST consume the actual result of its preceding tool invocation.
async fn actual_tool_read_two_requests_durable_evidence_usage_and_restart() {
    for partial in [false, true] {
        let mut lab = Lab::new(if partial {
            "native-tool-partial"
        } else {
            "native-tool-complete"
        })
        .await;
        // The complete committed task now carries its accepted metadata. Use
        // the existing metadata default for this positive observation fixture;
        // the explicit small task bound remains a separate refusal case below.
        let mut selected_input = input(now() + 60_000, &lab.limits);
        selected_input.limits.output_bytes = lab.limits.task_defaults.output_bytes;
        let submitted = task(
            lab.client
                .call(Operation::SubmitTask {
                    operation_id: "submit-native-tool".into(),
                    input: (selected_input).into(),
                })
                .await
                .unwrap(),
        );
        assert_eq!(submitted.input.limits.model_requests, 2);
        assert_eq!(
            submitted.input.limits.output_bytes,
            lab.limits.task_defaults.output_bytes
        );
        let mut peer = Peer::start(
            partial,
            false,
            submitted.id.clone(),
            accepted_limits(&submitted),
        )
        .await;
        let origin = lab.origin.clone();
        let worker = lab.start_native_worker("tool-inc-1", &origin, &peer.endpoint);
        let accepted = lab
            .until(&submitted.id, "native tool result accepted", |task| {
                task.status == TaskStatus::Succeeded
            })
            .await;
        let a = accepted.attempt.as_ref().unwrap();
        let result = accepted.result.as_ref().unwrap();
        assert_eq!(
            result.output,
            Some(
                json!({"task_id":accepted.id,"agent_id":accepted.assigned_agent_id,"attempt_id":a.id,"generation":a.generation})
            )
        );
        assert!(result.validation.contains(&contract::evidence(&a.id)));
        assert!(result.validation.iter().any(|s| s.contains(if partial {
            "subtotal for 1"
        } else {
            "complete reported total"
        })));
        let (tool_reply, tool) = receipt(&lab, &contract::call_id(&a.id, 1)).await;
        let request: Operation = serde_json::from_value(tool.intended.clone()).unwrap();
        let Operation::CallCollaboration { call } = &request else {
            panic!("tool occurrence was not persisted")
        };
        assert_eq!(call.tool, "collab_task_get");
        assert_eq!(call.arguments, json!({"task_id":accepted.id}));
        assert_eq!(call.caller.attempt_id, a.id);
        assert_eq!(call.caller.generation, a.generation);
        let observed: Outcome = serde_json::from_value(tool.outcome.clone().unwrap()).unwrap();
        let Outcome::Ok {
            data: Data::Task(actual),
        } = &observed
        else {
            panic!("original read outcome missing")
        };
        assert_eq!(actual.id, accepted.id);
        assert_eq!(actual.status, TaskStatus::Active);
        assert_eq!(actual.assigned_agent_id, accepted.assigned_agent_id);
        assert!(tool_reply.commit.as_ref().unwrap().controller_revision > 0);
        let (_, completed) = receipt(&lab, &contract::completion_id(&a.id)).await;
        let Operation::ObserveWorker { report } =
            serde_json::from_value(completed.intended).unwrap()
        else {
            panic!("missing typed completion")
        };
        assert!(
            matches!(report.observation,Observation::NativeLoopCompleted{ref requests, final_response_index: None, model_requests:2,ref calls} if requests.is_empty() && calls.len()==1 && calls[0].operation_id==call.operation_id && calls[0].logical_call_id==call.logical_call_id && calls[0].request_index==1)
        );
        let replay = lab.client.call(request).await.unwrap();
        assert_eq!(replay.commit, tool_reply.commit);
        let (reserved, _) = receipt(&lab, &format!("{}-native-request-2", a.id)).await;
        assert!(
            reserved.commit.unwrap().controller_revision
                > tool_reply.commit.as_ref().unwrap().controller_revision
        );
        let (_, usage) = receipt(
            &lab,
            &format!(
                "{}-native-{}-2",
                a.id,
                if partial { "request" } else { "usage" }
            ),
        )
        .await;
        let Operation::ObserveWorker { report } = serde_json::from_value(usage.intended).unwrap()
        else {
            panic!()
        };
        assert!(
            matches!(report.observation,Observation::Usage{model_requests:2,input_tokens:Some(i),output_tokens:Some(o), ..} if i==if partial{17}else{34} && o==if partial{9}else{18})
        );
        tokio::time::timeout(TIMEOUT, &mut peer.join)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(peer.requests.lock().unwrap().len(), 2);
        assert!(lab.starts.is_empty());
        lab.workers[worker].stop();
        lab.restart_controller().await;
        assert_eq!(lab.get(&accepted.id).await, accepted);
        assert_eq!(
            receipt(&lab, &contract::call_id(&a.id, 1)).await.0.commit,
            tool_reply.commit
        );
        save(lab.root.join("native-tool-accepted.json"), &accepted);
        lab.close();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancellation_or_deadline_after_first_post_prevents_native_tool_continuation() {
    for expired in [false, true] {
        let mut lab = Lab::new(if expired {
            "native-tool-deadline"
        } else {
            "native-tool-cancel"
        })
        .await;
        let submitted = submit_tool(&lab, now() + if expired { 4000 } else { 60_000 }).await;
        let mut peer = Peer::start(
            false,
            true,
            submitted.id.clone(),
            accepted_limits(&submitted),
        )
        .await;
        let origin = lab.origin.clone();
        lab.start_native_worker("tool-inc-1", &origin, &peer.endpoint);
        assert_eq!(peer.event().await, 1);
        if !expired {
            let current = lab.get(&submitted.id).await;
            let reply = lab
                .client
                .call(Operation::CancelTask {
                    operation_id: "cancel-tool".into(),
                    task_id: current.id,
                    expected_revision: current.revision,
                })
                .await
                .unwrap();
            assert!(reply.commit.is_some());
        }
        let terminal = lab
            .until(&submitted.id, "first request stopped", |t| {
                t.status
                    == if expired {
                        TaskStatus::Failed
                    } else {
                        TaskStatus::Cancelled
                    }
                    && t.attempt.as_ref().is_some_and(|a| a.stopped)
            })
            .await;
        assert!(terminal.result.is_none());
        if expired {
            assert_eq!(
                terminal.blocked_reason.as_deref(),
                Some("execution_deadline")
            );
        }
        let _ = peer.release.take().unwrap().send(());
        assert!(!matches!(
            tokio::time::timeout(Duration::from_millis(200), peer.events.recv()).await,
            Ok(Some(_))
        ));
        assert_eq!(peer.requests.lock().unwrap().len(), 1);
        let a = terminal.attempt.as_ref().unwrap();
        let second = lab
            .client
            .call(Operation::GetOperation {
                operation_id: format!("{}-native-request-2", a.id),
            })
            .await
            .unwrap();
        assert!(matches!(second.outcome,Outcome::Refused{ref code,..} if code=="not_found"));
        lab.close();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unacknowledged_actual_tool_call_withholds_second_post_and_retains_journal() {
    let mut lab = Lab::new("native-tool-unacknowledged").await;
    let mut proxy = Proxy::new(
        lab.origin.clone(),
        Fault::PendingToolObservation,
        lab.root.clone(),
        lab.limits.transport.body_bytes,
    )
    .await;
    let submitted = submit_tool(&lab, now() + 60_000).await;
    let peer = Peer::start(
        false,
        false,
        submitted.id.clone(),
        accepted_limits(&submitted),
    )
    .await;
    lab.start_native_worker("tool-inc-1", &proxy.url, &peer.endpoint);
    let held = proxy.event().await;
    assert_eq!(peer.requests.lock().unwrap().len(), 1);
    let Operation::CallCollaboration { call } = &held.original.command else {
        panic!()
    };
    let a = lab.get(&submitted.id).await.attempt.unwrap();
    assert_eq!(call.operation_id, contract::call_id(&a.id, 1));
    assert_eq!(call.tool, "collab_task_get");
    assert!(held.reply.commit.is_none());
    let journal = lab
        .root
        .join("native-worker")
        .join(".borg-worker")
        .join(borg_runtime::worker::process::component("native-worker"))
        .join("collaboration")
        .join(borg_runtime::worker::process::component(&a.id))
        .join("0000000001.json");
    let retained: Value = serde_json::from_slice(&fs::read(journal).unwrap()).unwrap();
    assert_eq!(
        retained["request"],
        serde_json::to_value(&held.original.command).unwrap()
    );
    assert!(retained["outcome"].is_null());
    assert!(retained["outcome_context"].is_null());
    assert_eq!(retained["consumed"], false);
    let current = lab.get(&submitted.id).await;
    task(
        lab.client
            .call(Operation::CancelTask {
                operation_id: "cancel-pending-tool".into(),
                task_id: current.id,
                expected_revision: current.revision,
            })
            .await
            .unwrap(),
    );
    held.release.unwrap().send(()).unwrap();
    lab.until(&submitted.id, "cancelled pending tool", |t| {
        t.status == TaskStatus::Cancelled && t.attempt.as_ref().is_some_and(|a| a.stopped)
    })
    .await;
    assert_eq!(peer.requests.lock().unwrap().len(), 1);
    let record = lab
        .client
        .call(Operation::GetOperation {
            operation_id: format!("{}-native-request-2", a.id),
        })
        .await
        .unwrap();
    assert!(matches!(record.outcome,Outcome::Refused{ref code,..} if code=="not_found"));
    lab.close();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn explicit_small_tool_output_bound_refuses_committed_observation_without_continuation() {
    let mut lab = Lab::new("native-tool-small-output-bound").await;
    let submitted = submit_tool(&lab, now() + 60_000).await;
    assert_eq!(submitted.input.limits.output_bytes, 4096);
    assert_eq!(submitted.input.limits.model_requests, 2);
    let mut peer = Peer::start(
        false,
        false,
        submitted.id.clone(),
        accepted_limits(&submitted),
    )
    .await;
    let origin = lab.origin.clone();
    let worker = lab.start_native_worker("tool-inc-1", &origin, &peer.endpoint);
    assert_eq!(peer.event().await, 1);
    let failed = lab
        .until(&submitted.id, "small tool output bound refused", |task| {
            task.status == TaskStatus::Failed && task.attempt.as_ref().is_some_and(|a| a.stopped)
        })
        .await;
    assert_eq!(failed.blocked_reason.as_deref(), Some("acceptance_failed"));
    assert!(failed.result.is_none());
    assert_eq!(failed.input.limits, submitted.input.limits);
    let attempt = failed.attempt.as_ref().unwrap();
    let (_, failure) = receipt(&lab, &format!("{}-report-00000000000000000001", attempt.id)).await;
    let Operation::ObserveWorker { report } = serde_json::from_value(failure.intended).unwrap()
    else {
        panic!("missing actual worker refusal");
    };
    assert!(
        matches!(report.observation, Observation::ExecutorOutcome { success: false, ref summary }
        if summary == "native collaboration result bound: task_output_bytes")
    );

    let (tool_reply, tool) = receipt(&lab, &contract::call_id(&attempt.id, 1)).await;
    let request: Operation = serde_json::from_value(tool.intended.clone()).unwrap();
    let Operation::CallCollaboration { call } = &request else {
        panic!("tool occurrence was not persisted");
    };
    assert_eq!(call.tool, "collab_task_get");
    assert_eq!(call.arguments, json!({"task_id":submitted.id}));
    let observed: Outcome = serde_json::from_value(tool.outcome.clone().unwrap()).unwrap();
    let Outcome::Ok {
        data: Data::Task(actual),
    } = &observed
    else {
        panic!("committed task read missing");
    };
    assert_eq!(actual.id, submitted.id);
    assert_eq!(actual.status, TaskStatus::Active);
    assert_eq!(
        actual.input.resolved_limits,
        submitted.input.resolved_limits
    );
    let wrapped = json!({"operation_id":call.operation_id,"logical_call_id":call.logical_call_id,
        "outcome":observed,"commit":tool_reply.commit});
    let encoded = serde_json::to_vec(&wrapped).unwrap();
    assert!(encoded.len() > submitted.input.limits.output_bytes);
    assert!(encoded.len() <= lab.limits.task_defaults.output_bytes);

    let journal_path = lab
        .root
        .join("native-worker/.borg-worker")
        .join(borg_runtime::worker::process::component("native-worker"))
        .join("collaboration")
        .join(borg_runtime::worker::process::component(&attempt.id))
        .join("0000000001.json");
    let journal_bytes = fs::read(&journal_path).unwrap();
    let journal: Value = serde_json::from_slice(&journal_bytes).unwrap();
    assert_eq!(journal["request"], serde_json::to_value(&request).unwrap());
    assert_eq!(journal["outcome"], tool.outcome.clone().unwrap());
    assert_eq!(
        journal["outcome_context"]["commit"],
        serde_json::to_value(&tool_reply.commit).unwrap()
    );
    assert_eq!(journal["consumed"], false);
    for operation_id in [
        contract::reservation_id(&attempt.id, 2),
        contract::completion_id(&attempt.id),
    ] {
        let absent = lab
            .client
            .call(Operation::GetOperation { operation_id })
            .await
            .unwrap();
        assert!(matches!(absent.outcome, Outcome::Refused { ref code, .. } if code == "not_found"));
    }
    let (_, usage) = receipt(&lab, &contract::usage_id(&attempt.id, 1)).await;
    let Operation::ObserveWorker { report } = serde_json::from_value(usage.intended).unwrap()
    else {
        panic!("first request usage missing");
    };
    assert!(matches!(
        report.observation,
        Observation::Usage {
            model_requests: 1,
            input_tokens: Some(17),
            output_tokens: Some(9), ..
        }
    ));
    assert!(!matches!(
        tokio::time::timeout(Duration::from_millis(200), peer.events.recv()).await,
        Ok(Some(_))
    ));
    assert_eq!(peer.requests.lock().unwrap().len(), 1);
    assert!(lab.starts.is_empty());
    lab.workers[worker].stop();
    lab.restart_controller().await;
    assert_eq!(lab.get(&failed.id).await, failed);
    assert_eq!(
        receipt(&lab, &contract::call_id(&attempt.id, 1))
            .await
            .0
            .commit,
        tool_reply.commit
    );
    assert_eq!(fs::read(&journal_path).unwrap(), journal_bytes);
    save(
        lab.root.join("native-tool-small-bound-refused.json"),
        &failed,
    );
    lab.close();
}
