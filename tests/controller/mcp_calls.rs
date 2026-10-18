//! Local real-controller and HTTP transactions, with controlled report producers.
//! These fixtures make no real provider/Codex/recovery-host qualification claim.
use super::*;
use borg_runtime::{
    mcp::{self, Caller, Invocation},
    native_loop_contract as loop_ids,
};
fn profiled(agent: &str) -> TaskInput {
    let mut t = native(agent);
    t.definition.execution_profile = Some(ExecutionProfile::CollaborationV1 { web_search: None });
    t
}
async fn fixture(c: &Controller, label: &str) -> (Task, Attempt, Caller) {
    let t = submit(c, &format!("{label}-submit"), profiled(label)).await;
    let w = worker(&format!("{label}-worker"), "inc", 1, 0);
    register(c, &w).await;
    let a = run(&commands(c, &w, vec![]).await);
    task(
        report(
            c,
            &a,
            &format!("{label}-start"),
            Observation::Started {
                thread_id: None,
                turn_id: None,
            },
        )
        .await,
    );
    let caller = Caller {
        task_id: t.id.clone(),
        attempt_id: a.id.clone(),
        generation: a.generation,
        worker_id: a.worker_id.clone(),
        incarnation: a.worker_incarnation.clone(),
    };
    (t, a, caller)
}
fn invocation(caller: &Caller, id: &str, tool: &str, args: serde_json::Value) -> Invocation {
    Invocation {
        operation_id: id.into(),
        logical_call_id: format!("logical-{id}"),
        caller: caller.clone(),
        tool: tool.into(),
        arguments: args,
    }
}
async fn invoke(c: &Controller, call: &Invocation) -> Reply {
    let r = c
        .handle(Request {
            protocol_revision: WIRE_REVISION,
            request_id: "mcp-test".into(),
            command: call.operation(),
        })
        .await;
    r.validate_for(&call.operation()).unwrap();
    r
}
async fn lookup(c: &Controller, call: &Invocation) -> Reply {
    let op = Operation::GetCollaborationCall {
        caller: call.caller.clone(),
        operation_id: call.operation_id.clone(),
    };
    let r = c
        .handle(Request {
            protocol_revision: WIRE_REVISION,
            request_id: "lookup".into(),
            command: op.clone(),
        })
        .await;
    r.validate_for(&op).unwrap();
    r
}
#[tokio::test]
async fn mcp_calls_replay_original_success_after_cancel_restart_and_conflict() {
    let p = path();
    let c = Controller::open_with_config(&p, config()).unwrap();
    let (t, _a, actor) = fixture(&c, "replay").await;
    let call = invocation(
        &actor,
        "memory-one",
        "collab_memory_set",
        json!({"key":"note","expected_revision":null,"value":{"answer":42}}),
    );
    let first = invoke(&c, &call).await;
    assert!(matches!(
        first.outcome,
        Outcome::Ok {
            data: Data::Memory(Some(_))
        }
    ));
    let current = get(&c, &t.id).await;
    task(
        call_fn(
            &c,
            Operation::CancelTask {
                operation_id: "cancel-replay".into(),
                task_id: t.id.clone(),
                expected_revision: current.revision,
            },
        )
        .await,
    );
    let mut changed = call.clone();
    changed.arguments["value"] = json!(43);
    assert_eq!(
        code(invoke(&c, &changed).await.outcome),
        "operation_conflict"
    );
    let next = invocation(
        &actor,
        "memory-new",
        "collab_memory_set",
        call.arguments.clone(),
    );
    assert!(matches!(
        invoke(&c, &next).await.outcome,
        Outcome::Refused { .. }
    ));
    drop(c);
    tokio::time::sleep(Duration::from_millis(50)).await;
    let c = Controller::open_with_config(&p, config()).unwrap();
    let again = invoke(&c, &call).await;
    assert_eq!(first.outcome, again.outcome);
    assert_eq!(first.commit, again.commit);
    let recovered = mcp::recovered(&call, lookup(&c, &call).await).unwrap();
    assert_eq!(recovered.reply.outcome, first.outcome);
    assert_eq!(recovered.reply.commit, first.commit);
    let mut other = call.clone();
    other.caller.worker_id = "another".into();
    assert_eq!(code(lookup(&c, &other).await.outcome), "caller_mismatch");
}
async fn call_fn(c: &Controller, op: Operation) -> Outcome {
    super::call(c, op).await
}
#[tokio::test]
async fn mcp_calls_new_occurrence_same_arguments_can_succeed_after_old_refusal() {
    let c = Controller::open_with_config(path(), config()).unwrap();
    let (_, _, actor) = fixture(&c, "occurrence").await;
    let first = invocation(&actor, "memory-before-create", "collab_memory_set",
        json!({"key":"later", "expected_revision":1, "value":"updated"}));
    let refused = invoke(&c, &first).await;
    assert!(matches!(refused.outcome, Outcome::Refused { .. }));
    assert!(refused.commit.is_some());
    let create = invocation(&actor, "create-memory", "collab_memory_set",
        json!({"key":"later", "expected_revision":null, "value":"initial"}));
    assert!(matches!(invoke(&c, &create).await.outcome, Outcome::Ok { data:Data::Memory(Some(_)) }));
    let replay = invoke(&c, &first).await;
    assert_eq!(refused.outcome, replay.outcome);
    assert_eq!(refused.commit, replay.commit);
    let mut second = first.clone();
    second.operation_id = "memory-after-create".into();
    second.logical_call_id = "later-intent".into();
    assert!(matches!(invoke(&c, &second).await.outcome, Outcome::Ok { data:Data::Memory(Some(_)) }));
}
#[tokio::test]
// Traceability scope: Actual controller calls reject stale generation/incarnation and expired progress without changing task output.
//= spec/impl/v2/implementation.md#shared-collaboration-tool-interface
//= type=test
//# The collaboration tool interface MUST validate its caller's current attempt before a new mutation.
async fn mcp_calls_refuse_stale_worker_deadline_and_unconfigured_without_effect() {
    let clock = Arc::new(AtomicU64::new(future() - 300000));
    let mut cfg = config();
    let cl = clock.clone();
    cfg.clock = Arc::new(move || cl.load(Ordering::SeqCst));
    let c = Controller::open_with_config(path(), cfg).unwrap();
    let (t, a, actor) = fixture(&c, "fences").await;
    let mut stale = invocation(
        &actor,
        "bad-owner",
        "collab_memory_set",
        json!({"key":"fence","expected_revision":null,"value":1}),
    );
    stale.caller.generation += 1;
    assert_eq!(code(invoke(&c, &stale).await.outcome), "stale_generation");
    stale.caller = actor.clone();
    stale.caller.incarnation = "wrong".into();
    stale.operation_id = "bad-inc".into();
    assert_eq!(code(invoke(&c, &stale).await.outcome), "stale_owner");
    clock.store(t.input.limits.deadline_unix_ms + 1, Ordering::SeqCst);
    let expired = invocation(
        &actor,
        "expired",
        "collab_task_progress",
        json!({"text":"never"}),
    );
    assert!(matches!(
        invoke(&c, &expired).await.outcome,
        Outcome::Refused { .. }
    ));
    assert_eq!(get(&c, &t.id).await.recent_output, "");
    let _ = a;
    // Missing profile now refuses at new admission; no grafted legacy state.
    let mut input=native("disabled");input.definition.execution_profile=None;
    assert_eq!(code(super::call(&c,Operation::SubmitTask{operation_id:"disabled".into(),input}).await),"template_profile_unbound");

}
#[tokio::test]
// Traceability scope: Unknown submission/protected-check tools and undeclared fields are refused by the closed collaboration catalogue; this does not qualify direct host/control-plane access.
//= spec/impl/v2/implementation.md#shared-collaboration-tool-interface
//= type=test
//# Operational queries MUST NOT grant authority to mutate worker controls or protected release state.
async fn mcp_calls_closed_catalogue_bounded_paging_and_model_receipt_query() {
    let c = Controller::open_with_config(path(), config()).unwrap();
    let (t, _, actor) = fixture(&c, "closed").await;
    for (i, tool, args) in [
        (1, "submit_task", json!({})),
        (2, "protected_check", json!({})),
        (
            3,
            "collab_task_progress",
            json!({"text":"ok","success":true}),
        ),
        (
            4,
            "collab_memory_set",
            json!({"key":"x","expected_revision":null,"value":"x".repeat(131073)}),
        ),
        (5, "collab_task_delegate", json!({"instruction":"do work"})),
    ] {
        let call = invocation(&actor, &format!("negative-{i}"), tool, args);
        assert!(matches!(
            invoke(&c, &call).await.outcome,
            Outcome::Refused { .. }
        ));
    }
    let read = invocation(
        &actor,
        "read-own",
        "collab_task_get",
        json!({"task_id":t.id}),
    );
    let original = invoke(&c, &read).await;
    let get_call = invocation(
        &actor,
        "read-receipt",
        "collab_call_get",
        json!({"call_id":"read-own"}),
    );
    let r = invoke(&c, &get_call).await;
    assert_eq!(r.commit.as_ref().unwrap().operation_id, "read-receipt");
    assert!(
        matches!(r.outcome,Outcome::Ok{data:Data::Operation(ref rec)}if rec.outcome==Some(serde_json::to_value(&original.outcome).unwrap()))
    );
    for i in 0..3 {
        submit(
            &c,
            &format!("page-submit-{i}"),
            native(&format!("page-agent-{i}")),
        )
        .await;
    }
    let mut cursor = None;
    let mut ids = vec![];
    for i in 0..8 {
        let call = invocation(
            &actor,
            &format!("page-{i}"),
            "collab_tasks_list",
            json!({"cursor":cursor,"limit":1,"agent_id":null,"parent_task_id":null}),
        );
        let r = invoke(&c, &call).await;
        let Outcome::Ok {
            data: Data::CollaborationPage(page),
        } = r.outcome
        else {
            panic!("{r:?}")
        };
        ids.extend(
            page.items
                .iter()
                .map(|v| v["id"].as_str().unwrap().to_owned()),
        );
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(ids.len(), 4);
    assert!(cursor.is_none());
}
async fn serve(c: Controller) -> (String, tokio::task::JoinHandle<Result<(), String>>) {
    let l = borg_runtime::server::listen("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let base = format!("http://{}", l.local_addr().unwrap());
    let b = base.clone();
    (
        base,
        tokio::spawn(async move { borg_runtime::server::serve(c, l, b).await }),
    )
}
#[tokio::test]
// Traceability scope: Actual HTTP/MCP memory mutation is reconciled from its original committed receipt; duplicate JSON is refused without creating a new call receipt.
//= spec/impl/v2/implementation.md#shared-collaboration-tool-interface
//= type=test
//# The collaboration tool interface MUST expose configured task-space mutations under their current operation rules.
//= spec/impl/v2/implementation.md#shared-collaboration-tool-interface
//= type=test
//# The collaboration tool interface MUST use the controller's canonical operation layer.
async fn mcp_calls_actual_http_initialization_duplicate_refusal_and_discarded_ack() {
    let c = Controller::open_with_config(path(), config()).unwrap();
    let (_, _, actor) = fixture(&c, "http").await;
    let (base, server) = serve(c.clone()).await;
    let client = mcp::Client::new(&base, actor.clone(), metadata()).unwrap();
    let tools = client.tools().await.unwrap();
    assert_eq!(tools.len(), 36);
    for name in ["collab_machines_list", "collab_machine_get"] {
        let tool = tools.iter().find(|tool| tool.name == name)
            .expect("accepted telemetry read remains in the closed catalogue");
        assert_eq!(tool.annotations, json!({"readOnlyHint":true,
            "destructiveHint":false,"idempotentHint":false,"openWorldHint":false}));
    }
    assert!(tools.iter().any(|tool| tool.name == "collab_read_continue"));
    assert!(tools.iter().any(|tool| tool.name == "collab_definition_update"));
    assert!(tools.iter().any(|tool|tool.name=="collab_limits_get"));
    assert_eq!(tools, mcp::catalogue(&metadata()));
    assert!(serde_json::to_vec(&tools).unwrap().len() < metadata().execution.native.helper_value_bytes);
    let call = invocation(
        &actor,
        "http-memory",
        "collab_memory_set",
        json!({"key":"note","expected_revision":null,"value":42}),
    );
    let http = reqwest::Client::builder().no_proxy().build().unwrap();
    let body = json!({"jsonrpc":"2.0","id":999,"method":"tools/call","params":{"name":call.tool,"arguments":call.arguments,"_meta":{"borg":{"operation_id":call.operation_id,"logical_call_id":call.logical_call_id}}}});
    let response = http
        .post(format!("{base}/mcp"))
        .header("x-borg-caller", serde_json::to_string(&actor).unwrap())
        .header("mcp-protocol-version", mcp::VERSION)
        .body(body.to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    drop(response); // Actual committed HTTP response deliberately not consumed.
    let recovered = client.get_call(&call).await.unwrap().unwrap();
    let replay = client.call(&call).await.unwrap();
    assert_eq!(recovered.reply.outcome, replay.reply.outcome);
    assert_eq!(recovered.reply.commit, replay.reply.commit);
    let bad = r#"{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"collab_memory_set","arguments":{"key":"a","key":"b","expected_revision":null,"value":1},"_meta":{"borg":{"operation_id":"duplicate","logical_call_id":"dup"}}}}"#;
    let reply = http
        .post(format!("{base}/mcp"))
        .header("x-borg-caller", serde_json::to_string(&actor).unwrap())
        .header("mcp-protocol-version", mcp::VERSION)
        .body(bad)
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_str(&reply).unwrap();
    assert_eq!(v["error"]["code"], -32700);
    let absent = invocation(&actor, "duplicate", "collab_memory_get", json!({"key":"b"}));
    assert!(client.get_call(&absent).await.unwrap().is_none());
    let response = http.get(format!("{base}/mcp")).send().await.unwrap();
    assert_eq!(response.status(), 405);
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
}
async fn reserve(c: &Controller, a: &Attempt, index: u32) {
    controlled_native_request(c, a, index, None, None).await;
}
#[tokio::test]
async fn mcp_calls_native_completion_requires_exact_ordered_caller_receipts() {
    let c = Controller::open_with_config(path(), config()).unwrap();
    let (t, a, actor) = fixture(&c, "evidence").await;
    reserve(&c, &a, 1).await;
    let call = invocation(
        &actor,
        &loop_ids::call_id(&a.id, 1),
        "collab_task_get",
        json!({"task_id":t.id}),
    );
    assert!(matches!(
        invoke(&c, &call).await.outcome,
        Outcome::Ok { .. }
    ));
    reserve(&c, &a, 2).await;
    let good = NativeCallEvidence { call_index: 1,
        request_index: 1,
        operation_id: call.operation_id.clone(),
        logical_call_id: call.logical_call_id.clone(),
    };
    for (i, calls) in [
        vec![],
        vec![NativeCallEvidence { call_index: 1,
            logical_call_id: "forged".into(),
            ..good.clone()
        }],
        vec![NativeCallEvidence { call_index: 1,
            request_index: 2,
            ..good.clone()
        }],
    ]
    .into_iter()
    .enumerate()
    {
        let o = report(
            &c,
            &a,
            &format!("bad-completion-{i}"),
            Observation::NativeLoopCompleted {
                requests: Vec::new(),
                final_response_index: None,
                model_requests: 2,
                calls,
            },
        )
        .await;
        assert!(matches!(o, Outcome::Refused { .. }));
    }
    task(
        report(
            &c,
            &a,
            &loop_ids::completion_id(&a.id),
            Observation::NativeLoopCompleted {
                requests: Vec::new(),
                final_response_index: None,
                model_requests: 2,
                calls: vec![good],
            },
        )
        .await,
    );
    task(
        report(
            &c,
            &a,
            "native-loop-exit",
            Observation::ExecutorOutcome {
                success: true,
                summary: "complete".into(),
            },
        )
        .await,
    );
    task(report(&c, &a, "native-loop-stop", Observation::ProcessTreeStopped).await);
    let result = task(
        report(
            &c,
            &a,
            "native-loop-result",
            Observation::ResultCandidate {
                summary: "accepted".into(),
                checks_passed: true,
                checkpoint: None,
                validation: vec![loop_ids::evidence(&a.id)],
                output: Some(json!({"answer":42})),
            },
        )
        .await,
    );
    assert_eq!(result.status, TaskStatus::Succeeded);
}
#[tokio::test]
async fn mcp_calls_result_only_and_out_of_order_native_evidence_cannot_accept() {
    let c = Controller::open_with_config(path(), config()).unwrap();
    let (t, a, actor) = fixture(&c, "forged").await;
    reserve(&c, &a, 1).await;
    reserve(&c, &a, 2).await;
    let call = invocation(
        &actor,
        &loop_ids::call_id(&a.id, 1),
        "collab_task_get",
        json!({"task_id":t.id}),
    );
    invoke(&c, &call).await;
    let calls = vec![NativeCallEvidence { call_index: 1,
        request_index: 1,
        operation_id: call.operation_id,
        logical_call_id: call.logical_call_id,
    }];
    assert_eq!(
        code(
            report(
                &c,
                &a,
                &loop_ids::completion_id(&a.id),
                Observation::NativeLoopCompleted {
                    requests: Vec::new(),
                    final_response_index: None,
                    model_requests: 2,
                    calls
                }
            )
            .await
        ),
        "source_mismatch"
    );
    task(
        report(
            &c,
            &a,
            "forged-exit",
            Observation::ExecutorOutcome {
                success: true,
                summary: "fake".into(),
            },
        )
        .await,
    );
    task(report(&c, &a, "forged-stop", Observation::ProcessTreeStopped).await);
    assert_eq!(
        code(
            report(
                &c,
                &a,
                "forged-result",
                Observation::ResultCandidate {
                    summary: "must refuse".into(),
                    checks_passed: true,
                    checkpoint: None,
                    validation: vec![loop_ids::evidence(&a.id)],
                    output: Some(json!({"answer":42}))
                }
            )
            .await
        ),
        "source_mismatch"
    );
    assert!(get(&c, &t.id).await.result.is_none());
}
#[tokio::test]
async fn mcp_calls_configured_child_is_exact_and_requires_parent_quiescence() {
    let c = Controller::open_with_config(path(), config()).unwrap();
    let mut input = profiled("parent");
    let mut child = coding("configured-child");
    child.definition.execution_profile =
        Some(ExecutionProfile::CollaborationV1 { web_search: None });
    child.limits.model_requests = 2;
    child.limits.result_bytes = 2048;
    child.limits.deadline_unix_ms = input.limits.deadline_unix_ms;
    input.native_workflow = Some(Box::new(NativeWorkflow::CodingThenContinue {
        child: CodingTaskTemplate { instruction_source: None,
            pull_request: None,
            instruction_prefix: "fixed".into(),
            definition: child.definition,
            repository: child.repository.unwrap(),
            acceptance_commands: child.acceptance_commands,
            dependencies: vec![],
            limits: child.limits,
        },
    }));
    let t = submit(&c, "configured-parent", input.clone()).await;
    let w = worker("delegate-worker", "inc", 1, 0);
    register(&c, &w).await;
    let a = run(&commands(&c, &w, vec![]).await);
    let actor = Caller {
        task_id: t.id.clone(),
        attempt_id: a.id.clone(),
        generation: a.generation,
        worker_id: a.worker_id.clone(),
        incarnation: a.worker_incarnation.clone(),
    };
    let call = invocation(
        &actor,
        "configured-delegate",
        "collab_task_delegate",
        json!({"instruction":"make the configured change"}),
    );
    let reply = invoke(&c, &call).await;
    let result = mcp::result(&call, reply.clone()).unwrap();
    assert!(result.requires_quiescence);
    let Outcome::Ok {
        data: Data::Task(child),
    } = reply.outcome
    else {
        panic!("{reply:?}")
    };
    assert_eq!(
        child.input,
        borg_runtime::delegation::coding_child(&t.input, "make the configured change").unwrap()
    );
    assert_eq!(child.parent_task_id, Some(t.id.clone()));
    let parent = get(&c, &t.id).await;
    assert_eq!(parent.waiting_child_id, Some(child.id.clone()));
    assert_eq!(parent.attempt.unwrap().phase, AttemptPhase::StopRequested);
    let next = invocation(
        &actor,
        "after-delegate",
        "collab_memory_set",
        json!({"key":"bad","expected_revision":null,"value":1}),
    );
    assert_eq!(code(invoke(&c, &next).await.outcome), "invalid_transition");
    let recovered = mcp::recovered(&call, lookup(&c, &call).await).unwrap();
    assert!(recovered.requires_quiescence);
    assert_eq!(recovered.reply.commit, result.reply.commit);
}
#[tokio::test]
async fn mcp_calls_cancel_uses_existing_cleanup_credit_without_granting_progress_credit() {
    let mut cfg = config();
    Arc::make_mut(&mut cfg.limits).controller.max_receipts = 5;
    let c = Controller::open_with_config(path(), cfg).unwrap();
    let (_, _, actor) = fixture(&c, "credit").await;
    let target = submit(&c, "credit-target", native("queued-credit")).await;
    let progress = invocation(
        &actor,
        "full-progress",
        "collab_task_progress",
        json!({"text":"no credit"}),
    );
    let refused = invoke(&c, &progress).await;
    assert_eq!(code(refused.outcome), "capacity");
    assert!(refused.commit.is_none());
    let cancel = invocation(
        &actor,
        "full-cancel",
        "collab_task_cancel",
        json!({"task_id":target.id,"expected_revision":target.revision}),
    );
    let answer = invoke(&c, &cancel).await;
    assert!(
        matches!(answer.outcome,Outcome::Ok{data:Data::Task(ref t)}if t.status==TaskStatus::Cancelled)
    );
    assert!(answer.commit.is_some());
    assert_eq!(answer.commit, invoke(&c, &cancel).await.commit);
}
#[tokio::test]
async fn mcp_calls_invalid_host_envelope_is_uncommitted_and_never_strands_a_receipt() {
    let c = Controller::open_with_config(path(), config()).unwrap();
    let (_, _, actor) = fixture(&c, "envelope").await;
    let mut call = invocation(
        &actor,
        "invalid-envelope",
        "collab_memory_get",
        json!({"key":"x"}),
    );
    call.logical_call_id = "".into();
    let reply = invoke(&c, &call).await;
    assert_eq!(code(reply.outcome), "invalid_input");
    assert!(reply.commit.is_none());
    assert_eq!(code(lookup(&c, &call).await.outcome), "not_found");
}

#[tokio::test]
async fn accepted_complete_result_bound_rolls_back_memory_mutation_and_replays_exact_refusal() {
    let p=path();let mut cfg=config();
    Arc::make_mut(&mut cfg.limits).execution.collaboration.complete_result_bytes=2048;
    let c=Controller::open_with_config(&p,cfg.clone()).unwrap();
    let (t,_,actor)=fixture(&c,"small-complete-result").await;
    let call=invocation(&actor,"large-memory-result","collab_memory_set",json!({"key":"large","expected_revision":null,"value":"x".repeat(8192)}));
    let first=invoke(&c,&call).await;
    assert_eq!(code(first.outcome.clone()),"response_bound");
    assert!(matches!(&first.outcome, Outcome::Refused { message, .. } if message.contains("execution.collaboration.complete_result_bytes")));
    assert!(first.commit.is_some(),"the bounded refusal must be durable");
    assert!(serde_json::to_vec(&mcp::result(&call,first.clone()).unwrap()).unwrap().len()<=2048);
    assert!(matches!(super::call(&c,Operation::GetMemory{agent_id:t.assigned_agent_id.clone(),key:"large".into()}).await,Outcome::Ok{data:Data::Memory(None)}),"no value from the refused mutation may commit");
    let replay=invoke(&c,&call).await;assert_eq!(replay.outcome,first.outcome);assert_eq!(replay.commit,first.commit);
    drop(c);controller_owner_released(&p).await;
    let c=Controller::open_with_config(&p,cfg).unwrap();
    let replay=invoke(&c,&call).await;assert_eq!(replay.outcome,first.outcome);assert_eq!(replay.commit,first.commit);
    let recovered=mcp::recovered(&call,lookup(&c,&call).await).unwrap();assert_eq!(recovered.reply.outcome,first.outcome);
    assert!(matches!(super::call(&c,Operation::GetMemory{agent_id:t.assigned_agent_id,key:"large".into()}).await,Outcome::Ok{data:Data::Memory(None)}));
}

#[tokio::test]
async fn multiple_call_completion_requires_exact_serial_ordinals_and_all_receipts() {
    for variant in ["valid","reversed","duplicate-ordinal","skipped-ordinal","reused-logical","wrong-request"] {
        let mut cfg=config();Arc::make_mut(&mut cfg.limits).execution.native.function_calls_per_response=2;
        let c=Controller::open_with_config(path(),cfg).unwrap();let (t,a,actor)=fixture(&c,variant).await;
        reserve(&c,&a,1).await;
        let mut calls=Vec::new();
        for ordinal in 1..=2 {
            let call=invocation(&actor,&loop_ids::call_operation_id(&a.id,1,ordinal),"collab_memory_get",json!({"key":"missing"}));
            let recorded=invoke(&c,&call).await;assert!(recorded.commit.is_some());
            calls.push(NativeCallEvidence{request_index:1,call_index:ordinal,operation_id:call.operation_id,logical_call_id:call.logical_call_id});
        }
        reserve(&c,&a,2).await;
        match variant {
            "reversed"=>calls.reverse(),
            "duplicate-ordinal"=>calls[1].call_index=1,
            "skipped-ordinal"=>calls[1].call_index=3,
            "reused-logical"=>calls[1].logical_call_id=calls[0].logical_call_id.clone(),
            "wrong-request"=>calls[1].request_index=2,
            "valid"=>{},_=>unreachable!(),
        }
        // Every variant uses the required exact completion identity on its own
        // database, so a different report ID cannot mask an evidence defect.
        let result=report(&c,&a,&loop_ids::completion_id(&a.id),Observation::NativeLoopCompleted{requests: Vec::new(), final_response_index: None, model_requests:2,calls}).await;
        if variant=="valid" {assert_eq!(task(result).id,t.id)} else {assert_eq!(code(result),"source_mismatch", "{variant}")}
    }
}

#[tokio::test]
// Traceability scope: New memory, progress and page calls after restart honor the accepted policy in both increased-default and decreased-default cases.
//= spec/impl/v2/implementation.md#limits-are-versioned-metadata
//= type=test
//# Changing default metadata MUST NOT silently change the limits of an already accepted task.
async fn accepted_new_calls_keep_memory_progress_and_page_policy_after_global_changes() {
    for accepted_large in [true, false] {
        let p = path();
        let mut cfg = config();
        let original = Arc::make_mut(&mut cfg.limits);
        original.revision = "accepted-policy".into();
        original.controller.memory_value_bytes = if accepted_large { 65536 } else { 1024 };
        original.controller.max_progress_bytes = if accepted_large { 65536 } else { 1024 };
        original.controller.max_list_tasks = if accepted_large { 100 } else { 1 };
        original.controller.collaboration_page_items = if accepted_large { 100 } else { 1 };
        original.product.page_items = if accepted_large { 100 } else { 1 };
        original.workflow.as_mut().expect("fixture workflow policy").page_items = original.controller.collaboration_page_items;
        let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
        let (t, _, actor) = fixture(&c, "accepted-policy").await;
        let accepted_snapshot = t.input.resolved_limits.clone();
        drop(c);
        controller_owner_released(&p).await;
        let later = Arc::make_mut(&mut cfg.limits);
        later.revision = "different-current-policy".into();
        later.controller.memory_value_bytes = if accepted_large { 1024 } else { 65536 };
        later.controller.max_progress_bytes = if accepted_large { 1024 } else { 65536 };
        later.controller.max_list_tasks = if accepted_large { 1 } else { 100 };
        later.controller.collaboration_page_items = if accepted_large { 1 } else { 100 };
        later.product.page_items = if accepted_large { 1 } else { 100 };
        later.workflow.as_mut().expect("fixture workflow policy").page_items = later.controller.collaboration_page_items;
        let c = Controller::open_with_config(&p, cfg).unwrap();
        let memory = invocation(&actor, "new-policy-memory", "collab_memory_set",
            json!({"key":"policy-value","expected_revision":null,"value":"x".repeat(2048)}));
        let first = invoke(&c, &memory).await;
        assert!(first.commit.is_some());
        if accepted_large {
            assert!(matches!(first.outcome, Outcome::Ok { data: Data::Memory(Some(_)) }));
        } else {
            assert_eq!(code(first.outcome.clone()), "invalid_input");
            assert!(matches!(&first.outcome, Outcome::Refused { message, .. } if message.contains("controller.memory_value_bytes")));
        }
        let progress = invocation(&actor, "new-policy-progress", "collab_task_progress",
            json!({"text":"x".repeat(2048)}));
        let observed = invoke(&c, &progress).await;
        if accepted_large {
            let current = get(&c, &t.id).await;
            assert_eq!(current.progress, "x".repeat(2048));
            assert_eq!(observed.outcome, Outcome::Ok {
                data: Data::CollaborationPage(mcp::ListPage {
                    items: vec![json!({
                        "task_id": current.id,
                        "session_id": current.session_id,
                        "revision": current.revision,
                    })],
                    next_cursor: None,
                }),
            });
        } else {
            assert_eq!(code(observed.outcome.clone()), "capacity");
            assert!(matches!(&observed.outcome, Outcome::Refused { message, .. } if message.contains("controller.max_progress_bytes")));
        }
        let page = invocation(&actor, "new-policy-page", "collab_memory_list",
            json!({"cursor":null,"limit":2}));
        let paged = invoke(&c, &page).await;
        if accepted_large {
            assert!(matches!(paged.outcome, Outcome::Ok { data: Data::Collaboration(_) }));
        } else {
            assert_eq!(code(paged.outcome), "invalid_input");
        }
        let stored = get(&c, &t.id).await;
        assert_eq!(stored.input.resolved_limits, accepted_snapshot);
        let read = super::call(&c, Operation::GetMemory {
            agent_id: t.assigned_agent_id, key: "policy-value".into(),
        }).await;
        if accepted_large {
            assert!(matches!(read, Outcome::Ok { data: Data::Memory(Some(v)) } if v.value == json!("x".repeat(2048))));
        } else {
            assert!(matches!(read, Outcome::Ok { data: Data::Memory(None) }));
        }
        let replay = invoke(&c, &memory).await;
        assert_eq!(replay.outcome, first.outcome);
        assert_eq!(replay.commit, first.commit);
    }
}

//= spec/impl/v2/implementation.md#limits-are-versioned-metadata
//= type=test
//# The shared agent interface MUST expose the effective limits applicable to its caller.
#[tokio::test]
// Traceability scope: Actual controller acceptance and active-caller tool results assert global and agent revisions, retained override values, explicit task limits and exact read replay after changed defaults.
//= spec/impl/v2/implementation.md#limits-are-versioned-metadata
//= type=test
//# An accepted task MUST identify the metadata revisions used to resolve its execution limits.
async fn active_limits_tool_separates_current_defaults_from_accepted_overrides_after_restart() {
    use borg_runtime::{
        collaboration::QueryResult,
        limits::{ExecutionLimitOverrides, LimitSnapshot, RuntimeLimits},
    };

    fn assert_view(reply: &Reply, current: &RuntimeLimits, accepted: &LimitSnapshot) {
        let Outcome::Ok {
            data: Data::Collaboration(QueryResult::Limits { global, execution }),
        } = &reply.outcome
        else {
            panic!("expected the active caller's limits view: {reply:?}");
        };
        assert_eq!(global.as_ref(), current);
        assert_eq!(execution.as_deref(), Some(accepted));
        assert!(reply.commit.is_some(), "the tool observation has an exact receipt");
    }

    let p = path();
    let mut cfg = config();
    Arc::make_mut(&mut cfg.limits).revision = "limits-tool-global-before".into();
    let original_global = cfg.limits.as_ref().clone();
    let mut input = profiled("limits-tool-agent");
    input.definition.revision = "limits-tool-agent-revision-7".into();
    let mut native_override = original_global.execution.native.clone();
    native_override.context_bytes *= 2;
    native_override.function_calls_per_response = 2;
    input.definition.limit_overrides = Some(Box::new(ExecutionLimitOverrides {
        native: Some(native_override.clone()),
        ..ExecutionLimitOverrides::default()
    }));
    input.limits.model_requests = 3;
    input.limits.output_bytes = 2048;
    input.limits.result_bytes = 4096;
    input.limits.max_delegation_depth = 2;
    let explicit_task_limits = input.limits.clone();
    let expected_snapshot = original_global.resolve_task(&input).unwrap();
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    let submitted = submit(&c, "limits-tool-submit", input.clone()).await;
    assert_eq!(submitted.input.resolved_limits.as_ref(), Some(&expected_snapshot));
    assert_eq!(expected_snapshot.global_revision, original_global.revision);
    assert_eq!(expected_snapshot.agent_revision, input.definition.revision);
    assert_eq!(expected_snapshot.values.native, native_override);
    assert_eq!(expected_snapshot.values.coding, original_global.execution.coding);
    let w = worker("limits-tool-worker", "limits-tool-incarnation", 1, 0);
    register(&c, &w).await;
    let attempt = run(&commands(&c, &w, vec![]).await);
    assert_eq!(attempt.task_id, submitted.id);
    task(report(&c, &attempt, "limits-tool-start", Observation::Started {
        thread_id: None,
        turn_id: None,
    }).await);
    let actor = Caller {
        task_id: submitted.id.clone(),
        attempt_id: attempt.id.clone(),
        generation: attempt.generation,
        worker_id: attempt.worker_id.clone(),
        incarnation: attempt.worker_incarnation.clone(),
    };
    let other = submit(&c, "limits-tool-other-submit", profiled("limits-tool-other")).await;
    let before_call = invocation(&actor, "limits-before-restart", "collab_limits_get", json!({}));
    let before = invoke(&c, &before_call).await;
    assert_view(&before, &original_global, &expected_snapshot);
    let retained_input = serde_json::to_vec(&get(&c, &submitted.id).await.input).unwrap();
    let retained_other_input = serde_json::to_vec(&get(&c, &other.id).await.input).unwrap();

    drop(c);
    controller_owner_released(&p).await;
    let later = Arc::make_mut(&mut cfg.limits);
    later.revision = "limits-tool-global-after".into();
    later.execution.native.context_bytes *= 3;
    later.execution.native.function_calls_per_response = 3;
    later.execution.coding.progress_chars += 17;
    later.task_defaults.model_requests = 7;
    later.validate().unwrap();
    let current_global = later.clone();
    let c = Controller::open_with_config(&p, cfg).unwrap();
    let after_call = invocation(&actor, "limits-after-restart", "collab_limits_get", json!({}));
    let after = invoke(&c, &after_call).await;
    assert_view(&after, &current_global, &expected_snapshot);
    assert_ne!(current_global.execution.native, expected_snapshot.values.native);
    assert_ne!(current_global.execution.coding, expected_snapshot.values.coding);
    assert_ne!(current_global.task_defaults.model_requests, explicit_task_limits.model_requests);
    // A new occurrence observes current defaults; replay keeps the original view.
    let replay = invoke(&c, &before_call).await;
    assert_eq!(replay.outcome, before.outcome);
    assert_eq!(replay.commit, before.commit);

    let task_call = invocation(&actor, "limits-explicit-task-read", "collab_task_get",
        json!({"task_id":submitted.id}));
    let observed_task = task(invoke(&c, &task_call).await.outcome);
    assert_eq!(observed_task.input.limits, explicit_task_limits);
    assert_eq!(observed_task.input.definition.limit_overrides, input.definition.limit_overrides);
    assert_eq!(observed_task.input.resolved_limits.as_ref(), Some(&expected_snapshot));
    assert_eq!(serde_json::to_vec(&observed_task.input).unwrap(), retained_input);

    for (id, arguments) in [
        ("limits-arbitrary-task", json!({"task_id":other.id})),
        ("limits-write-shaped", json!({"limits":{"execution":{"native":{"context_bytes":1}}}})),
    ] {
        let refused = invoke(&c, &invocation(&actor, id, "collab_limits_get", arguments)).await;
        assert_eq!(code(refused.outcome), "invalid_input");
    }
    // The calls may retain receipts, but neither read nor rejected arguments may
    // mutate current policy, accepted metadata, overrides, or explicit task limits.
    let final_call = invocation(&actor, "limits-after-refusals", "collab_limits_get", json!({}));
    let final_view = invoke(&c, &final_call).await;
    assert_view(&final_view, &current_global, &expected_snapshot);
    assert_eq!(final_view.outcome, after.outcome);
    assert_eq!(c.limits(), &current_global);
    assert_eq!(serde_json::to_vec(&get(&c, &submitted.id).await.input).unwrap(), retained_input);
    assert_eq!(serde_json::to_vec(&get(&c, &other.id).await.input).unwrap(), retained_other_input);
}

#[tokio::test]
// Traceability scope: Real controller MCP receipts and recovery/restart projection checks; private keys/root never occur in returned JSON.
//= spec/impl/v2/implementation.md#repository-associations-are-versioned-metadata
//= type=test
//# A model-visible repository projection MUST NOT expose controller storage keys.
async fn repository_catalogue_mcp_safe_projection_currentness_and_original_replay() {
    use borg_runtime::collaboration::QueryResult;
    fn entry(reply: &Reply) -> &borg_runtime::repository_catalogue::Entry {
        let Outcome::Ok { data: Data::Collaboration(QueryResult::RepositoryRegistration(entry)) } = &reply.outcome else {
            panic!("expected repository metadata: {reply:?}")
        };
        assert!(reply.commit.is_some());
        entry
    }
    let p = path();
    let mut cfg = config();
    let private_root = p.parent().unwrap().join("private-catalogue-store-root");
    cfg.repository_read.store_root = Some(private_root.clone());
    let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
    let mut registration = fixture_repository_registration();
    registration.repository_id = "catalogue-mcp".into();
    registration.source_store_key = "private-catalogue-source-key".into();
    registration.checkpoint_store_key = "private-catalogue-checkpoint-key".into();
    register_repository_fixture(&c, &registration).await;
    let (t, _, actor) = fixture(&c, "catalogue-mcp-reader").await;
    let list = invocation(&actor, "catalogue-list-original", "collab_repositories_list", json!({"after":null,"limit":1}));
    let original_list = invoke(&c, &list).await;
    let Outcome::Ok { data: Data::Collaboration(QueryResult::Repositories(page)) } = &original_list.outcome else {
        panic!("expected repository page: {original_list:?}")
    };
    assert!(original_list.commit.is_some());
    assert_eq!(page.items.len(), 1);
    assert!(page.next_cursor.is_none());
    assert_eq!(page.items[0].repository_id, registration.repository_id);
    let current = invocation(&actor, "catalogue-current-original", "collab_repository_get", json!({"repository_id":registration.repository_id,"registration_revision":null}));
    let original_current = invoke(&c, &current).await;
    assert_eq!(&page.items[0], entry(&original_current));
    assert_eq!(entry(&original_current).current_revision, "revision-1");
    let exact = invocation(&actor, "catalogue-exact-original", "collab_repository_get", json!({"repository_id":registration.repository_id,"registration_revision":"revision-1"}));
    let original_exact = invoke(&c, &exact).await;
    assert_eq!(entry(&original_exact), entry(&original_current));
    for reply in [&original_list, &original_current, &original_exact] {
        let serialized = serde_json::to_string(reply).unwrap();
        for private in ["source_store_key", "checkpoint_store_key", "store_root", registration.source_store_key.as_str(), registration.checkpoint_store_key.as_str(), private_root.to_str().unwrap()] {
            assert!(!serialized.contains(private), "private catalogue input leaked: {private}");
        }
    }
    let mut revised = registration.clone();
    revised.registration_revision = "revision-2".into();
    revised.source_endpoint = "https://example.invalid/revised.git".into();
    assert!(matches!(super::call(&c, Operation::ReviseRepository {
        operation_id: "catalogue-revise".into(),
        expected_generation: entry(&original_current).generation,
        registration: revised.clone(),
    }).await, Outcome::Ok { data: Data::RepositoryRegistration(_) }));
    let latest = invocation(&actor, "catalogue-current-new", "collab_repository_get", current.arguments.clone());
    let latest_reply = invoke(&c, &latest).await;
    assert_eq!(entry(&latest_reply).registration_revision, "revision-2");
    assert_eq!(entry(&latest_reply).source_endpoint, revised.source_endpoint);
    let historical = invocation(&actor, "catalogue-exact-new", "collab_repository_get", exact.arguments.clone());
    let historical_reply = invoke(&c, &historical).await;
    assert_eq!(entry(&historical_reply).registration_revision, "revision-1");
    assert_eq!(entry(&historical_reply).source_endpoint, registration.source_endpoint);
    assert_eq!(entry(&historical_reply).current_revision, "revision-2");
    // Original occurrences retain the original safe observation across a head
    // change; fresh occurrences observe the new current head.
    for (call, original) in [(&list, &original_list), (&current, &original_current), (&exact, &original_exact)] {
        let replay = invoke(&c, call).await;
        assert_eq!(replay.outcome, original.outcome);
        assert_eq!(replay.commit, original.commit);
    }
    let active = get(&c, &t.id).await;
    task(super::call(&c, Operation::CancelTask {
        operation_id: "catalogue-reader-cancel".into(),
        task_id: t.id,
        expected_revision: active.revision,
    }).await);
    drop(c);
    controller_owner_released(&p).await;
    let c = Controller::open_with_config(&p, cfg).unwrap();
    for (call, original) in [(&list, &original_list), (&current, &original_current), (&exact, &original_exact)] {
        let replay = invoke(&c, call).await;
        assert_eq!(replay.outcome, original.outcome);
        assert_eq!(replay.commit, original.commit);
        let recovered = mcp::recovered(call, lookup(&c, call).await).unwrap();
        assert_eq!(recovered.reply.outcome, original.outcome);
        assert_eq!(recovered.reply.commit, original.commit);
    }
    for (id, tool, arguments) in [
        ("catalogue-list-after-cancel", list.tool.clone(), list.arguments.clone()),
        ("catalogue-get-after-cancel", current.tool.clone(), current.arguments.clone()),
    ] {
        let fresh = invocation(&actor, id, &tool, arguments);
        assert_eq!(code(invoke(&c, &fresh).await.outcome), "invalid_transition");
    }
}

// Local diagnostic reconstruction of installed self-delivery task123. Only the
// new local controller's generated task/attempt/session identities are mapped.
// Exact original input, selected limits, tool/argument/occurrence values remain
// frozen artifact inputs. No executor, repository IO, model or remote runs.
#[tokio::test]
async fn self_delivery_missing_task_target_is_a_committed_http_refusal() {
    let original: Operation = serde_json::from_str(include_str!("fixtures/self_delivery/original-request.json")).unwrap();
    let Operation::SubmitTask { input, .. } = original else {
        panic!("fixture operation")
    };
    let selected: borg_runtime::limits::RuntimeLimits = serde_json::from_str(include_str!("fixtures/self_delivery/original-limits.json")).unwrap();
    let projection: serde_json::Value = serde_json::from_str(include_str!("fixtures/self_delivery/original-invocation-projection.json")).unwrap();
    let original_call: Invocation =
        serde_json::from_value(projection["entries"][0]["invocation"].clone()).unwrap();
    assert_eq!(original_call.tool, "collab_task_get");
    assert_eq!(
        original_call.arguments,
        json!({"task_id":"task-f2dc0f0ed413f702c9fe9516f4ef47ef"})
    );
    assert_ne!(
        original_call.arguments["task_id"],
        original_call.caller.task_id
    );
    assert!(serde_json::to_vec(&input).unwrap().len() > 70_000);
    for small_complete_result in [false, true] {
        let p = path();
        let mut cfg = config();
        let mut policy = selected.clone();
        if small_complete_result {
            policy.execution.collaboration.complete_result_bytes = 2048;
        }
        cfg.limits = Arc::new(policy.clone());
        let now = input.limits.deadline_unix_ms - 1_800_000;
        cfg.clock = Arc::new(move || now);
        let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
        register_repository_fixture(
            &c,
            &borg_runtime::repository_catalogue::Registration {
                pull_request_target: None,
                repository_id: "runtime".into(),
                registration_revision: "1".into(),
                display_name: "Local diagnostic runtime".into(),
                source_endpoint: "git://127.0.0.1:19418/runtime.git".into(),
                checkpoint_endpoint: "git://127.0.0.1:19418/runtime.git".into(),
                source_store_key: "diagnostic-source".into(),
                checkpoint_store_key: "diagnostic-checkpoint".into(),
            },
        )
        .await;
        let t = submit(&c, "isolated-original-input", input.clone()).await;
        let w = worker(
            &original_call.caller.worker_id,
            &original_call.caller.incarnation,
            0,
            1,
        );
        register(&c, &w).await;
        let a = run(&commands(&c, &w, vec![]).await);
        task(
            report(
                &c,
                &a,
                "isolated-start",
                Observation::Started {
                    thread_id: Some("01a140bf-0e6a-7330-b800-fca150f10b66".into()),
                    turn_id: Some("01a140bf-0f9c-7dc0-a1e2-90373aaf1075".into()),
                },
            )
            .await,
        );
        let mut call = original_call.clone();
        call.caller.task_id = t.id.clone();
        call.caller.attempt_id = a.id.clone();
        call.caller.generation = a.generation;
        let (base, server) = serve(c.clone()).await;
        let client = mcp::Client::new(&base, call.caller.clone(), policy).unwrap();
        client.tools().await.unwrap();
        let first = client.call(&call).await.unwrap();
        assert!(
            matches!(&first.reply.outcome, Outcome::Refused { code, message } if code == "not_found" && message == "task not found")
        );
        assert_eq!(
            first.reply.commit.as_ref().unwrap().operation_id,
            original_call.operation_id
        );
        assert!(!first.requires_quiescence);
        assert_eq!(
            client.get_call(&call).await.unwrap().unwrap().reply.commit,
            first.reply.commit
        );
        let mut correct = call.clone();
        correct.operation_id = "isolated-correct-target".into();
        correct.logical_call_id = "isolated-correct-target".into();
        correct.arguments = json!({"task_id":t.id});
        let corrected = client.call(&correct).await.unwrap();
        assert!(corrected.reply.commit.is_some());
        if small_complete_result {
            assert!(
                matches!(&corrected.reply.outcome, Outcome::Refused { code, message } if code == "response_bound" && message.contains("execution.collaboration.complete_result_bytes"))
            );
        } else {
            assert!(
                matches!(&corrected.reply.outcome, Outcome::Ok { data: Data::Task(got) } if got.id == t.id && got.input.acceptance_commands == input.acceptance_commands)
            );
        }
        assert_eq!(
            client
                .get_call(&correct)
                .await
                .unwrap()
                .unwrap()
                .reply
                .outcome,
            corrected.reply.outcome
        );
        server.abort();
        let _ = server.await;
        drop(client);
        drop(c);
        controller_owner_released(&p).await;
        let reopened = Controller::open_with_config(&p, cfg).unwrap();
        let retained = mcp::recovered(&call, lookup(&reopened, &call).await).unwrap();
        assert_eq!(retained.reply.outcome, first.reply.outcome);
        assert_eq!(retained.reply.commit, first.reply.commit);
    }
}

// Closed configuration application uses this module's actual caller fixtures.
include!("definition_updates.rs");

#[path = "mcp_read_retention.rs"]
mod read_retention;

#[path = "mcp_task_progress.rs"]
mod task_progress;
