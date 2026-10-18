//! Controlled Native Responses and the real Coding stdio MCP bridge share the
//! actual controller. No provider, installed Codex, deployment or review vote claim.
#[path = "../identity_support.rs"]
mod support;
use borg_runtime::charter::*;
use borg_runtime::{
    mcp::{self, CallResult, Client, Tool},
    native::{
        self, CharterExchange, CharterInput, NativeFunction, NativeLoopCall, NativeLoopProfile,
        NativeLoopReply, NativeLoopStep, ValidationHost, ValidationRuntime,
    },
    protocol::*,
    worker::{codex_mcp, process},
};
use serde_json::{Value, json};
use std::{process::Stdio, sync::Arc, time::Duration};
use support::*;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

// Supplied only by the controlled fixture environment, never a provider key.
// HOME is unsuitable here: committed task results contain repository paths.
const FIXTURE_KEY_ENV: &str = "BORG_IDENTITY_TEST_CREDENTIAL";
const FIXTURE_KEY: &str = "synthetic-charter-identity-loop-key";

struct Server {
    url: String,
    join: tokio::task::JoinHandle<Result<(), String>>,
}
impl Server {
    async fn new(h: &Harness) -> Self {
        let listener = borg_runtime::server::listen("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let c = h.c.clone();
        let origin = url.clone();
        let join =
            tokio::spawn(async move { borg_runtime::server::serve(c, listener, origin).await });
        Self { url, join }
    }
    async fn close(self) {
        self.join.abort();
        assert!(self.join.await.unwrap_err().is_cancelled());
    }
}
fn client(h: &Harness, s: &Server) -> Client {
    Client::new_for_profile(
        &s.url,
        h.caller.clone(),
        (*h.original
            .input
            .resolved_limits
            .as_ref()
            .unwrap()
            .global_metadata)
            .clone(),
        h.original
            .input
            .definition
            .execution_profile
            .clone()
            .unwrap(),
    )
    .unwrap()
}
async fn native_call(
    h: &Harness,
    c: &Client,
    tools: &[Tool],
    previous: &mut Option<(NativeLoopCall, String)>,
    tool: &str,
    args: Value,
    index: u32,
) -> CallResult {
    assert_eq!(
        std::env::var(FIXTURE_KEY_ENV).as_deref(),
        Ok(FIXTURE_KEY),
        "the qualification environment must select the explicit synthetic credential"
    );
    let profile = NativeLoopProfile {
        functions: tools
            .iter()
            .map(|t| NativeFunction {
                name: t.name.clone(),
                description: t.description.clone(),
                parameters: t.input_schema.clone(),
            })
            .collect(),
        web_search: None,
    };
    let response = json!({"id":format!("controlled-response-{index}"),"status":"completed","usage":{"input_tokens":5,"output_tokens":3,"total_tokens":8},"output":[{"type":"function_call","id":format!("item-{index}"),"status":"completed","call_id":format!("native-call-{index}"),"name":tool,"arguments":args.to_string()}]});
    let peer = super::Fixture::start(
        200,
        serde_json::to_vec(&response).unwrap(),
        Duration::ZERO,
        "",
    )
    .await;
    let input = h.current().await.input;
    let bounds = input.execution_limits().unwrap().native.clone();
    let path = h.root.join(id("native-helper"));
    std::fs::create_dir(&path).unwrap();
    let caller = &h.caller;
    ok(api(
        &h.c,
        Operation::ObserveWorker {
            report: Report {
                report_id: borg_runtime::native_loop_contract::reservation_id(
                    &caller.attempt_id,
                    index,
                ),
                worker_id: caller.worker_id.clone(),
                incarnation: caller.incarnation.clone(),
                task_id: caller.task_id.clone(),
                attempt_id: caller.attempt_id.clone(),
                generation: caller.generation,
                observation: Observation::Usage {
                    model_requests: index,
                    input_tokens: None,
                    output_tokens: None,
                    native_diagnostic_reserved: true,
                },
            },
        },
    )
    .await);
    let Data::CharterQuery(CharterQueryResult::Session(Some(effective))) = ok(api(
        &h.c,
        Operation::QueryCharter {
            query: CharterQuery::Session {
                session_id: h.original.session_id.clone(),
            },
        },
    )
    .await) else {
        panic!("accepted charter session")
    };
    let turn_id = id("native-boundary");
    let Data::Charter(CharterOutcome::Boundary(boundary)) = ok(api(
        &h.c,
        Operation::ObserveCharter {
            operation_id: turn_id.clone(),
            caller: caller.clone(),
            observation: CharterObservation::BeginTurn {
                session_id: h.original.session_id.clone(),
                expected_revision: effective.session.revision,
                request_index: index,
            },
        },
    )
    .await) else {
        panic!("actual native turn admission")
    };
    let source = &boundary.effective.source;
    let exchange = Arc::new(
        CharterExchange::new(
            CharterInput {
                revision: serde_json::to_value(&boundary.effective.reference).unwrap(),
                source: serde_json::to_string(source).unwrap(),
                changed: false,
            },
            &bounds,
        )
        .unwrap(),
    );
    let host = ValidationHost::new(
        env!("CARGO_BIN_EXE_borg-runtime").into(),
        path,
        Arc::new(bounds),
        Arc::new(ValidationRuntime::new(Arc::new(h.cfg.limits.worker.clone())).unwrap()),
    )
    .with_charter(exchange.clone());
    let prior = previous.as_ref().map(|(call, output)| {
        (
            call,
            NativeLoopReply {
                call_id: call.call_id(),
                output,
            },
        )
    });
    let observation = native::execute_loop_step(
        &input,
        &native::NativeConfig {
            endpoint: peer.endpoint.clone(),
            api_key_env: FIXTURE_KEY_ENV.into(),
            model: SELECTED_MODEL.into(),
        },
        &profile,
        prior,
        &host,
        async { Ok(()) },
        std::future::pending(),
    )
    .await;
    let step = match observation.result {
        Ok(step) => step,
        Err(error) => {
            // A pre-dispatch refusal must remain visible; waiting for a request
            // that was never sent otherwise hides it behind the peer timeout.
            peer.handle.abort();
            let _ = peer.handle.await;
            panic!("actual native request {index} failed: {error:?}");
        }
    };
    let request = peer.received.await.unwrap();
    peer.handle.await.unwrap();
    assert!(
        !request.to_string().contains(FIXTURE_KEY),
        "the fixture credential must never enter the actual model request body"
    );
    assert_eq!(
        request["tools"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|t| t["type"] == "function")
            .count(),
        tools.len()
    );
    let expected = tools.iter().find(|t| t.name == tool).unwrap();
    assert_eq!(
        request["tools"]
            .as_array()
            .unwrap()
            .iter()
            .find(|t| t["name"] == tool)
            .unwrap()["parameters"],
        expected.input_schema
    );
    if let Some((prior, output)) = previous.as_ref() {
        let exact =
            json!({"type":"function_call_output","call_id":prior.call_id(),"output":output});
        assert_eq!(
            request["input"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|item| **item == exact)
                .count(),
            1,
            "the next actual model request receives its preceding committed result once"
        );
    }
    let NativeLoopStep::Call(call) = step else {
        panic!("actual native tool call required")
    };
    assert_eq!(call.name(), tool);
    assert_eq!(call.arguments(), &args);
    assert_eq!(call.request_index(), index);
    let original = mcp::Invocation {
        operation_id: borg_runtime::native_loop_contract::call_operation_id(
            &h.caller.attempt_id,
            index,
            1,
        ),
        logical_call_id: call.call_id().into(),
        caller: h.caller.clone(),
        tool: call.name().into(),
        arguments: call.arguments().clone(),
    };
    let captured = exchange.take_reply().unwrap().unwrap();
    assert_eq!(captured.call_ids, vec![call.call_id().to_string()]);
    assert!(captured.original_output.is_some());
    ok(api(
        &h.c,
        Operation::ObserveCharter {
            operation_id: id("native-reply"),
            caller: caller.clone(),
            observation: CharterObservation::Reply {
                turn_id: turn_id.clone(),
                reply: CharterReplyObservation {
                    output: captured.original_output,
                    output_reference: None,
                    response_id: captured.response_id,
                    output_item_ids: captured.item_ids,
                    tool_call_ids: captured.call_ids,
                    declaration: captured.declared,
                },
            },
        },
    )
    .await);
    let result = c.call(&original).await.unwrap();
    assert!(result.reply.commit.is_some());
    assert_eq!(c.call(&original).await.unwrap(), result);
    let lookup = api(
        &h.c,
        Operation::GetCollaborationCall {
            caller: h.caller.clone(),
            operation_id: original.operation_id.clone(),
        },
    )
    .await;
    assert_eq!(
        mcp::recovered(&original, lookup).unwrap().reply.outcome,
        result.reply.outcome
    );
    let closed = ok(api(
        &h.c,
        Operation::ObserveCharter {
            operation_id: id("native-complete-turn"),
            caller: caller.clone(),
            observation: CharterObservation::CompleteTurn {
                turn_id,
                results: vec![CharterResultReceipt::Collaboration {
                    call_id: call.call_id().into(),
                    operation_id: original.operation_id.clone(),
                }],
            },
        },
    )
    .await);
    assert!(
        matches!(closed,Data::Charter(CharterOutcome::Turn(ref turn)) if turn.completed_by.is_some())
    );
    assert_eq!(
        native::validation_cleanup_pending(),
        0,
        "actual Native helper process closure"
    );
    let output=serde_json::to_string(&json!({"operation_id":original.operation_id,"logical_call_id":original.logical_call_id,"outcome":result.reply.outcome,"commit":result.reply.commit})).unwrap();
    *previous = Some((call, output));
    result
}
struct CodingPeer {
    child: tokio::process::Child,
    input: Option<tokio::process::ChildStdin>,
    output: BufReader<tokio::process::ChildStdout>,
    config: codex_mcp::Config,
    sequence: u64,
}
impl CodingPeer {
    async fn start(h: &Harness, s: &Server) -> Self {
        let task = h.current().await;
        let state = h.root.join("coding-bridge");
        process::private_directory(&state).unwrap();
        process::atomic_json(
            &state.join("borg-thread.json"),
            &json!({"thread_id":"controlled-coding-thread","turn_start_sent":true}),
        )
        .unwrap();
        let config = codex_mcp::Config {
            instruction_source: task.input.instruction_source.clone(),
            execution_profile: task.input.definition.execution_profile.clone(),
            runtime_limits: (*task.input.resolved_limits.as_ref().unwrap().global_metadata).clone(),
            limits: task.input.execution_limits().unwrap().collaboration.clone(),
            worker_limits: h.cfg.limits.worker.clone(),
            controller_url: s.url.clone(),
            caller: h.caller.clone(),
            deadline_unix_ms: task.input.limits.deadline_unix_ms,
            state,
        };
        let config_path = config.state.join("bridge.json");
        process::atomic_json(&config_path, &config).unwrap();
        let log = std::fs::File::create(config.state.join("process.log")).unwrap();
        let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_borg-runtime"))
            .args(["__collaboration_mcp", "--config"])
            .arg(&config_path)
            .arg("--config-bytes")
            .arg(config.limits.config_bytes.to_string())
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .env("HOME", &config.state)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(log)
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let input = child.stdin.take();
        let output = BufReader::new(child.stdout.take().unwrap());
        let mut peer = Self {
            child,
            input,
            output,
            config,
            sequence: 0,
        };
        let init=peer.rpc("initialize",json!({"protocolVersion":mcp::VERSION,"capabilities":{},"clientInfo":{"name":"controlled-coding","version":"1"}})).await;
        assert_eq!(init["protocolVersion"], mcp::VERSION);
        peer
    }
    async fn rpc(&mut self, method: &str, params: Value) -> Value {
        self.sequence += 1;
        let mut frame = serde_json::to_vec(
            &json!({"jsonrpc":"2.0","id":self.sequence,"method":method,"params":params}),
        )
        .unwrap();
        frame.push(b'\n');
        self.input
            .as_mut()
            .unwrap()
            .write_all(&frame)
            .await
            .unwrap();
        self.input.as_mut().unwrap().flush().await.unwrap();
        let mut line = Vec::new();
        let n = tokio::time::timeout(
            Duration::from_secs(20),
            self.output.read_until(b'\n', &mut line),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(
            n > 0,
            "bridge closed before response; inspect {}",
            self.config.state.display()
        );
        let response: Value = serde_json::from_slice(&line).unwrap();
        assert!(response.get("error").is_none(), "{response:?}");
        assert_eq!(response["id"], self.sequence);
        response["result"].clone()
    }
    async fn call(&mut self, name: &str, args: Value, logical: &str) -> CallResult {
        let reply=self.rpc("tools/call",json!({"name":name,"arguments":args,"_meta":{"callId":logical,"threadId":"controlled-coding-thread","sessionId":"controlled-coding-session"}})).await;
        let outcome: CallResult =
            serde_json::from_str(reply["content"][0]["text"].as_str().unwrap()).unwrap();
        self.rpc("ping", json!({})).await;
        let journal: Value = serde_json::from_slice(
            &std::fs::read(self.config.directory().join("calls.json")).unwrap(),
        )
        .unwrap();
        let entry = journal["entries"]
            .as_array()
            .unwrap()
            .iter()
            .find(|e| e["invocation"]["logical_call_id"] == logical)
            .unwrap();
        assert_eq!(entry["delivered"], true);
        assert_eq!(entry["outcome"], serde_json::to_value(&outcome).unwrap());
        outcome
    }
    async fn close(mut self) {
        drop(self.input.take());
        let status = tokio::time::timeout(Duration::from_secs(20), self.child.wait())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            status.code(),
            Some(1),
            "bridge EOF is a recorded stop, not task success"
        );
        let stop: Value =
            serde_json::from_slice(&std::fs::read(self.config.stop_path()).unwrap()).unwrap();
        assert_eq!(
            stop["caller"],
            serde_json::to_value(&self.config.caller).unwrap()
        );
    }
}
#[tokio::test]
async fn native_and_coding_task_creation_share_real_mcp_contract() {
    let native = Harness::new("native-task-create", "triage", ExecutorKind::Native, false).await;
    let ns = Server::new(&native).await;
    let nc = client(&native, &ns);
    let nt = nc.tools().await.unwrap();
    assert_eq!(nt.len(), 67);
    let mut previous = None;
    let before = native.current().await;
    let created = task(
        native_call(
            &native,
            &nc,
            &nt,
            &mut previous,
            "collab_task_create",
            create_args(None),
            1,
        )
        .await
        .reply,
    );
    assert_eq!(created.assigned_agent_id, "triage");
    assert_eq!(created.input.definition, before.input.definition);
    assert_ne!(created.id, before.id);
    assert_ne!(created.session_id, before.session_id);
    assert!(created.parent_task_id.is_none());
    assert_eq!(
        creation(&native, &created.id).await.creator,
        bound_actor(&native)
    );
    // The same Native caller can request ordinary Coding work for a known
    // identity/source. It acquires no delegated wait or release authority.
    let mut args = create_args(Some("engineer"));
    args["repository"] = json!(native.source);
    args["instruction_source"] = json!(native.source);
    args["output_schema"] = Value::Null;
    let coding_from_native = task(
        native_call(
            &native,
            &nc,
            &nt,
            &mut previous,
            "collab_task_create",
            args,
            2,
        )
        .await
        .reply,
    );
    assert_eq!(
        coding_from_native.input.definition,
        selected(&native.c, "engineer").await.definition
    );
    assert_sources(&coding_from_native, &native.source);
    assert!(coding_from_native.parent_task_id.is_none());
    assert_eq!(
        creation(&native, &coding_from_native.id).await.creator,
        bound_actor(&native)
    );
    let after = native.current().await;
    assert_eq!(after.input, before.input);
    assert_eq!(after.status, before.status);
    assert_eq!(after.waiting_child_id, before.waiting_child_id);
    assert_eq!(native.tasks().await.len(), 3);
    let coding = Harness::new(
        "coding-task-create",
        "engineer",
        ExecutorKind::Coding,
        false,
    )
    .await;
    let cs = Server::new(&coding).await;
    let mut peer = CodingPeer::start(&coding, &cs).await;
    let catalogue: Vec<Tool> =
        serde_json::from_value(peer.rpc("tools/list", json!({})).await["tools"].clone()).unwrap();
    // Coding's existing bridge excludes delegation. All other tool contracts,
    // including the complete independent-task creation schema, stay identical.
    assert!(nt.iter().any(|tool| tool.name == "collab_task_delegate"));
    assert!(
        !catalogue
            .iter()
            .any(|tool| tool.name == "collab_task_delegate")
    );
    assert_eq!(
        nt.iter().find(|tool| tool.name == "collab_task_create"),
        catalogue
            .iter()
            .find(|tool| tool.name == "collab_task_create")
    );
    assert_eq!(
        nt.iter()
            .filter(|tool| tool.name != "collab_task_delegate")
            .collect::<Vec<_>>(),
        catalogue.iter().collect::<Vec<_>>()
    );
    let mut args = create_args(Some("engineer"));
    args["repository"] = json!(coding.source);
    args["instruction_source"] = json!(coding.source);
    args["output_schema"] = Value::Null;
    args["acceptance_commands"] = json!([["/usr/bin/true"]]);
    let cbefore = coding.current().await;
    let result = peer
        .call("collab_task_create", args.clone(), "coding-task-create")
        .await;
    let ct = task(result.reply.clone());
    assert_eq!(ct.input.definition, cbefore.input.definition);
    assert_sources(&ct, &coding.source);
    assert_ne!(ct.session_id, cbefore.session_id);
    assert!(ct.parent_task_id.is_none());
    assert_eq!(
        creation(&coding, &ct.id).await.creator,
        bound_actor(&coding)
    );
    assert_eq!(coding.current().await, cbefore);
    assert_eq!(
        peer.call("collab_task_create", args, "coding-task-create")
            .await,
        result
    );
    assert_eq!(coding.tasks().await.len(), 2);
    peer.close().await;
    cs.close().await;
    ns.close().await;
    drop(nc);
    native.close().await;
    coding.close().await;
}
fn assert_sources(task: &Task, source: &TaskRepositorySelection) {
    let repository = task.input.repository.as_ref().unwrap();
    assert_eq!(repository.registration.as_ref(), Some(&source.registration));
    assert_eq!(repository.start_commit, source.start_commit);
    let instructions = task.input.instruction_source.as_ref().unwrap();
    assert_eq!(
        instructions.registration.as_ref(),
        Some(&source.registration)
    );
    assert_eq!(instructions.start_commit, source.start_commit);
}
fn native_invocation(h: &Harness, index: u32, tool: &str, args: Value) -> mcp::Invocation {
    mcp::Invocation {
        operation_id: borg_runtime::native_loop_contract::call_operation_id(
            &h.caller.attempt_id,
            index,
            1,
        ),
        logical_call_id: format!("native-call-{index}"),
        caller: h.caller.clone(),
        tool: tool.into(),
        arguments: args,
    }
}
#[tokio::test]
async fn charter_creates_designer_and_requests_review_through_mcp() {
    let h = Harness::new("charter-designer", "charter", ExecutorKind::Native, true).await;
    let server = Server::new(&h).await;
    let client = client(&h, &server);
    let tools = client.tools().await.unwrap();
    assert_eq!(tools.len(), 74);
    assert_eq!(tools.iter().filter(|tool| tool.name == "collab_read_continue").count(), 1);
    let mut previous = None;
    let designer = definition(&h.cfg.limits, "ui-designer", ExecutorKind::Native, false);
    let args = admin_args(&h.cfg.limits, json!({"definition":designer}));
    let registration_call = native_invocation(&h, 1, "collab_agent_register", args.clone());
    let registered = native_call(
        &h,
        &client,
        &tools,
        &mut previous,
        "collab_agent_register",
        args,
        1,
    )
    .await;
    let Outcome::Ok {
        data: Data::IdentityAdministration(ref r),
    } = registered.reply.outcome
    else {
        panic!("registration")
    };
    assert_eq!(r.actor, bound_actor(&h));
    assert!(matches!(&r.effect,IdentityEffect::Registered(a) if a.definition==designer));
    let mut args = create_args(Some("ui-designer"));
    args["instruction"] = json!(
        "Review the human collaboration workflow for usability using this exact known source. Creation is not review completion or a release approval."
    );
    args["repository_read"] = json!(h.source);
    args["expected_agent_revision"] = json!("1");
    let creation_call = native_invocation(&h, 2, "collab_task_create", args.clone());
    let created = native_call(
        &h,
        &client,
        &tools,
        &mut previous,
        "collab_task_create",
        args,
        2,
    )
    .await;
    let review = task(created.reply.clone());
    assert_eq!(review.assigned_agent_id, "ui-designer");
    assert_eq!(review.input.definition, designer);
    assert_eq!(review.status, TaskStatus::Queued);
    assert!(review.result.is_none() && review.parent_task_id.is_none());
    assert_ne!(review.session_id, h.original.session_id);
    let source = review.input.repository_read.as_ref().unwrap();
    assert_eq!(source.registration.as_ref(), Some(&h.source.registration));
    assert_eq!(source.start_commit, h.source.start_commit);
    let doc=native_call(&h,&client,&tools,&mut previous,"collab_document_create",json!({"content":{"title":"Review scope","body":"Inspect the human collaboration workflow."},"supplied_content_id":null,"responsible_owner":{"kind":"agent","agent_id":"ui-designer"}}),3).await;
    assert!(matches!(
        doc.reply.outcome,
        Outcome::Ok {
            data: Data::DocumentMutation(_)
        }
    ));
    assert_eq!(h.tasks().await.len(), 2);
    let provenance = creation(&h, &review.id).await;
    assert_eq!(provenance.operation_id, creation_call.operation_id);
    assert_eq!(provenance.logical_call_id, creation_call.logical_call_id);
    assert_eq!(provenance.creator, bound_actor(&h));
    server.close().await;
    drop(client);
    let cfg = h.cfg.clone();
    let h = h.reopen(cfg).await;
    assert_eq!(selected(&h.c, "ui-designer").await.definition, designer);
    for (original, result) in [
        (&registration_call, &registered),
        (&creation_call, &created),
    ] {
        let replay = h.invoke(original).await;
        assert_eq!(replay.outcome, result.reply.outcome);
        assert_eq!(replay.commit, result.reply.commit);
    }
    let retained = task(
        api(
            &h.c,
            Operation::GetTask {
                task_id: review.id.clone(),
            },
        )
        .await,
    );
    assert_eq!(retained, review);
    assert_eq!(creation(&h, &review.id).await, provenance);
    assert_eq!(h.tasks().await.len(), 2);
    h.close().await;
}

// Controlled Responses select the real native call; the Coding peer runs the
// real stdio bridge. Both reach the same canonical Controller operation layer.
// This qualifies adapters and retained receipts, not a provider or Codex model.
#[tokio::test]
async fn identity_limit_fields_native_and_coding_share_schema_commits_and_original_replay() {
    use borg_runtime::limits::DefinitionLimitField::{CodingFrameBytes, CollaborationJournalBytes};
    let fields = [CodingFrameBytes, CollaborationJournalBytes];
    let mut p = policy();
    p.execution.coding.rate_limit_retry = Some(borg_runtime::supplied_outer::RetryPolicy {
        initial_backoff_ms: 3000, backoff_multiplier: 2, max_backoff_ms: 60000, jitter_ms: 1000,
    });
    p.definition_updates.as_mut().unwrap().allowed_fields.extend(fields);
    p.validate().unwrap();
    let n = Harness::with_policy("fields-native", "charter", ExecutorKind::Native, true, p.clone()).await;
    let c = Harness::with_policy("fields-coding", "charter", ExecutorKind::Coding, true, p).await;
    let ns = Server::new(&n).await;
    let cs = Server::new(&c).await;
    let nc = client(&n, &ns);
    let tools = nc.tools().await.unwrap();
    let mut peer = CodingPeer::start(&c, &cs).await;
    let coding_tools = peer.rpc("tools/list", json!({})).await;
    let nschema = &tools.iter().find(|t| t.name == "collab_definition_update").unwrap().input_schema;
    let cschema = &coding_tools["tools"].as_array().unwrap().iter()
        .find(|t| t["name"] == "collab_definition_update").unwrap()["inputSchema"];
    assert_eq!(nschema, cschema);
    for name in ["coding.frame_bytes", "collaboration.journal_bytes"] {
        assert_eq!(nschema["properties"]["changes"]["properties"][name], json!({"type":"integer","minimum":1}));
    }
    assert_eq!(nschema["properties"]["changes"]["additionalProperties"], false);
    let ni = n.current().await.input;
    let ci = c.current().await.input;
    let mut previous = None;
    let mut originals = Vec::new();
    for (index, delta, classification) in [(1u32, 1isize, ConfigurationChangeClass::Widening), (2, -2, ConfigurationChangeClass::Narrowing)] {
        let mut outcomes = Vec::new();
        for (h, coding) in [(&n, false), (&c, true)] {
            let before = selected(&h.c, "engineer").await.definition;
            let limits = h.cfg.limits.resolve_definition(&before).unwrap();
            let changes: std::collections::BTreeMap<_, _> = fields.iter().map(|f|
                (*f, f.value(&limits).unwrap().checked_add_signed(delta).unwrap())).collect();
            let args = json!({"agent_id":before.id,"expected_revision":before.revision,
                "expected_global_revision":h.cfg.limits.revision,
                "expected_policy_revision":h.cfg.limits.definition_updates.as_ref().unwrap().revision,"changes":changes});
            let logical = format!("identity-limit-fields-{index}");
            let result = if coding {
                let first = peer.call("collab_definition_update", args.clone(), &logical).await;
                assert_eq!(peer.call("collab_definition_update", args.clone(), &logical).await, first);
                first
            } else {
                native_call(h, &nc, &tools, &mut previous, "collab_definition_update", args.clone(), index).await
            };
            let call = if coding {
                let journal: Value = serde_json::from_slice(&std::fs::read(peer.config.directory().join("calls.json")).unwrap()).unwrap();
                serde_json::from_value(journal["entries"].as_array().unwrap().iter()
                    .find(|e| e["invocation"]["logical_call_id"] == logical).unwrap()["invocation"].clone()).unwrap()
            } else { native_invocation(h, index, "collab_definition_update", args) };
            let Outcome::Ok { data: Data::DefinitionUpdate(ref update) } = result.reply.outcome else { panic!("canonical update: {:?}", result.reply) };
            assert_eq!(update.changed_fields, fields);
            assert_eq!(update.classification, classification);
            assert_eq!(update.effective_for, ConfigurationEffect::FutureSubmissions);
            let after = h.cfg.limits.resolve_definition(&update.selected_definition).unwrap();
            let mut expected = limits.clone();
            for (field,value) in changes { field.set(&mut expected,value); }
            assert_eq!(after, expected);
            assert_eq!(after.coding.rate_limit_retry, limits.coding.rate_limit_retry);
            assert_eq!(after.coding.model_context_window, limits.coding.model_context_window);
            assert_eq!(after.collaboration.capabilities, limits.collaboration.capabilities);
            assert_eq!(selected(&h.c, "engineer").await.definition, update.selected_definition);
            outcomes.push(after);
            originals.push((coding, call, result));
        }
        assert_eq!(outcomes[0], outcomes[1], "same composed resource change across adapters");
    }
    assert_eq!(n.current().await.input, ni);
    assert_eq!(c.current().await.input, ci);
    peer.close().await;
    ns.close().await; cs.close().await; drop(nc);
    let ncfg = n.cfg.clone(); let ccfg = c.cfg.clone();
    let n = n.reopen(ncfg).await; let c = c.reopen(ccfg).await;
    for (coding, call, result) in originals {
        let h = if coding { &c } else { &n };
        let replay = h.invoke(&call).await;
        assert_eq!(replay.outcome, result.reply.outcome);
        assert_eq!(replay.commit, result.reply.commit);
    }
    assert_eq!(n.current().await.input, ni);
    assert_eq!(c.current().await.input, ci);
    n.close().await; c.close().await;
}
