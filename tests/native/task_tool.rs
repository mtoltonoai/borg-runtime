use super::*;
use borg_runtime::{
    native::{ValidationHost, ValidationRuntime, continue_current_task, request_current_task},
    task_tool::{CurrentTaskProjection, ToolObservation},
};
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
fn input() -> TaskInput {
    let mut value = task();
    value.limits.model_requests = 2;
    value.limits.deadline_unix_ms = now_ms() + 30_000;
    value.limits.output_bytes = 4096;
    value.native_workflow = Some(Box::new(NativeWorkflow::InspectCurrentTaskThenContinue {}));
    bind(value)
}
fn host() -> ValidationHost {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let root = std::env::temp_dir().join(format!(
        "borg-tool-validator-{}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&root).unwrap();
    let limits = borg_runtime::limits::RuntimeLimits::seed().unwrap();
    ValidationHost::new(
        PathBuf::from(env!("CARGO_BIN_EXE_borg-runtime")),
        root,
        Arc::new(limits.execution.native),
        Arc::new(ValidationRuntime::new(Arc::new(limits.worker)).unwrap()),
    )
}
fn function() -> Value {
    json!({"type":"function_call","status":"completed","call_id":"call_fixture","name":"get_current_task","arguments":"{}"})
}
fn response(call: Value) -> Value {
    with_usage(
        json!({"status":"completed","output":[{"type":"reasoning","summary":[],"encrypted_content":"synthetic opaque context"},{"type":"message","status":"completed","role":"assistant","content":[{"type":"output_text","text":"Inspecting this task"}]},call]}),
    )
}

#[tokio::test]
//= spec/impl/v2/implementation.md#the-native-executor-is-bounded
//= type=test
//# A native tool invocation MUST be restricted to the task's configured tool interface.
async fn configured_tool_refuses_selectors_other_tools_extra_calls_and_refusal_context() {
    let mut wrong = function();
    wrong["name"] = json!("shell");
    let mut selector = function();
    selector["arguments"] = json!("{\"task_id\":\"another-task\"}");
    let mut unfinished = function();
    unfinished["status"] = json!("in_progress");
    let mut duplicate = response(function());
    duplicate["output"].as_array_mut().unwrap().push(function());
    let mut refusal = response(function());
    refusal["output"][1]["content"][0] = json!({"type":"refusal","refusal":"no"});
    let mut unknown = response(function());
    unknown["output"]
        .as_array_mut()
        .unwrap()
        .push(json!({"type":"web_search_call","status":"completed"}));
    for envelope in [
        response(wrong),
        response(selector),
        response(unfinished),
        duplicate,
        refusal,
        unknown,
    ] {
        let f = Fixture::start(
            200,
            serde_json::to_vec(&envelope).unwrap(),
            Duration::ZERO,
            "",
        )
        .await;
        let value = request_current_task(
            &input(),
            &config(f.endpoint.clone()),
            &host(),
            async { Ok(()) },
            std::future::pending(),
        )
        .await;
        assert!(value.result.is_err());
        assert_eq!(
            value.request_exposure,
            RequestExposure::HttpResponseReceived
        );
        assert_eq!(value.usage, reported());
        assert_eq!(value.provider_outcome, ProviderOutcome::Unknown);
    }
}

#[tokio::test]
async fn tool_workflow_unconfigured_budget_and_after_preflight_currentness_refuse_before_post() {
    for kind in 0..3 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let cfg = config(format!(
            "http://{}/v1/responses",
            listener.local_addr().unwrap()
        ));
        let mut value = input();
        if kind == 0 {
            value.native_workflow = None;
        }
        if kind == 1 {
            value.limits.model_requests = 1;
        }
        let value = bind(value);
        let called = Arc::new(AtomicBool::new(false));
        let flag = called.clone();
        let result = request_current_task(
            &value,
            &cfg,
            &host(),
            async move {
                flag.store(true, Ordering::Relaxed);
                Err("controller-currentness-refused".into())
            },
            std::future::pending(),
        )
        .await;
        assert!(result.result.is_err());
        assert_eq!(result.request_exposure, RequestExposure::NotStarted);
        assert_eq!(result.provider_outcome, ProviderOutcome::NotStarted);
        assert_eq!(called.load(Ordering::Relaxed), kind == 2);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), listener.accept())
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn tool_followup_rejects_wrong_call_or_source_and_repeated_tool_in_final() {
    let value = input();
    let h = host();
    let f = Fixture::start(
        200,
        serde_json::to_vec(&response(function())).unwrap(),
        Duration::ZERO,
        "",
    )
    .await;
    let result = request_current_task(
        &value,
        &config(f.endpoint.clone()),
        &h,
        async { Ok(()) },
        std::future::pending(),
    )
    .await;
    assert_eq!(result.provider_outcome, ProviderOutcome::Completed);
    let call = result.result.unwrap();
    let request = f.received.await.unwrap();
    assert_eq!(request["model"], "gpt-6-astra");
    assert_eq!(request["reasoning"], json!({"effort":"max"}));
    assert_eq!(
        request["max_output_tokens"],
        value.execution_limits().unwrap().native.max_output_tokens
    );
    let original = ToolObservation {
        call_id: call.call_id().into(),
        task: CurrentTaskProjection {
            task_id: "task".into(),
            agent_id: value.definition.id.clone(),
            definition: value.definition.clone(),
            status: TaskStatus::Active,
            observed_controller_revision: 1,
            deadline_unix_ms: value.limits.deadline_unix_ms,
            attempt_id: "attempt".into(),
            generation: 1,
        },
    };
    for mismatch in 0..3 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let cfg = config(format!(
            "http://{}/v1/responses",
            listener.local_addr().unwrap()
        ));
        let mut changed = original.clone();
        match mismatch {
            0 => changed.call_id = "wrong".into(),
            1 => changed.task.definition.revision = "wrong".into(),
            _ => changed.task.deadline_unix_ms -= 1,
        }
        let output = continue_current_task(
            &value,
            &cfg,
            &call,
            &changed,
            &h,
            async { Ok(()) },
            std::future::pending(),
        )
        .await;
        assert_eq!(
            output.result.err().unwrap(),
            "native_tool_observation_mismatch"
        );
        assert_eq!(output.request_exposure, RequestExposure::NotStarted);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), listener.accept())
                .await
                .is_err()
        );
    }
    let f = Fixture::start(
        200,
        serde_json::to_vec(&response(function())).unwrap(),
        Duration::ZERO,
        "",
    )
    .await;
    let output = continue_current_task(
        &value,
        &config(f.endpoint.clone()),
        &call,
        &original,
        &h,
        async { Ok(()) },
        std::future::pending(),
    )
    .await;
    assert!(output.result.is_err());
    assert_eq!(output.provider_outcome, ProviderOutcome::Unknown);
    let request = f.received.await.unwrap();
    assert_eq!(request["model"], "gpt-6-astra");
    assert_eq!(request["reasoning"], json!({"effort":"max"}));
    assert_eq!(
        request["max_output_tokens"],
        value.execution_limits().unwrap().native.max_output_tokens
    );
    assert_eq!(request["tools"], json!([]));
    assert_eq!(request["tool_choice"], "none");
}

#[test]
fn tool_workflow_wire_is_opt_in_closed_and_does_not_enable_coding_delegation() {
    let value = input();
    borg_runtime::delegation::validate_workflow(&value).unwrap();
    let workflow = serde_json::to_value(value.native_workflow.as_deref().unwrap()).unwrap();
    assert_eq!(
        workflow,
        json!({"kind":"inspect_current_task_then_continue"})
    );
    assert!(
        serde_json::from_value::<NativeWorkflow>(
            json!({"kind":"inspect_current_task_then_continue","task_id":"another"})
        )
        .is_err()
    );
    assert!(borg_runtime::delegation::template(&value).is_none());
    assert!(borg_runtime::delegation::coding_child(&value, "instruction").is_err());
}

#[tokio::test]
async fn task_tool_astra_selection_refuses_previous_model_before_network() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut cfg = config(format!(
        "http://{}/v1/responses",
        listener.local_addr().unwrap()
    ));
    cfg.model = "gpt-6.1-sol".into();
    let out = request_current_task(
        &input(),
        &cfg,
        &host(),
        async { Ok(()) },
        std::future::pending(),
    )
    .await;
    assert_eq!(
        out.result.err().as_deref(),
        Some("native_config_template_mismatch")
    );
    assert_eq!(out.request_exposure, RequestExposure::NotStarted);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), listener.accept())
            .await
            .is_err()
    );
}
