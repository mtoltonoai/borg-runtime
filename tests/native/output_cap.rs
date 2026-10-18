//! Actual same-binary helper and controlled loopback Responses peers only.
//! No provider interoperability, worker retry or controller commit claim.
use super::*;
use borg_runtime::{
    limits::{NativeCircuitLimits, NativeRetryLimits, RuntimeLimits},
    native::{
        NativeLoopProfile, NativeLoopStep, ValidationHost, ValidationRuntime,
        execute_loop_step_with_host_inputs_at,
    },
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

fn accepted() -> (TaskInput, RuntimeLimits) {
    let mut limits = metadata();
    limits.native_retry = Some(NativeRetryLimits {
        initial_backoff_ms: 5,
        backoff_multiplier: 2,
        max_backoff_ms: 20,
        jitter_ms: 1,
        circuit: NativeCircuitLimits {
            failure_threshold: 2,
            open_ms: 50,
            max_entries: 8,
        },
        auth_cooldown_ms: 80,
        auth_max_entries: 8,
        coordination_record_bytes: 65_536,
    });
    let mut input = task();
    // Preserve the parent fixture's five-second deadline and one-request budget.
    input.resolved_limits = Some(limits.resolve_task(&input).unwrap());
    (input, limits)
}

fn empty_profile() -> NativeLoopProfile {
    NativeLoopProfile { functions: vec![], web_search: None }
}

fn host(program: &LinkedProgram, limits: &RuntimeLimits) -> ValidationHost {
    ValidationHost::new(
        program.path.clone(),
        program.directory.join("processes"),
        Arc::new(limits.execution.native.clone()),
        Arc::new(ValidationRuntime::new(Arc::new(limits.worker.clone())).unwrap()),
    )
}

fn capped(items: Value) -> Value {
    with_usage(json!({
        "id": "bounded-cap-fixture",
        "status": "incomplete",
        "error": null,
        "incomplete_details": {"reason": "max_output_tokens"},
        "output": items,
    }))
}

async fn join_peer(mut peer: Fixture, input: &TaskInput) {
    let request = tokio::time::timeout(Duration::from_secs(5), &mut peer.received)
        .await.unwrap().unwrap();
    assert_eq!(request["model"], "gpt-6-astra");
    assert_eq!(request["max_output_tokens"], input.execution_limits().unwrap().native.max_output_tokens);
    assert_eq!(request["text"]["format"]["schema"], input.output_schema.clone().unwrap());
    assert_eq!(request["stream"], false);
    // Join the actual one-POST peer; do not use Fixture::finish's abort path.
    tokio::time::timeout(Duration::from_secs(5), peer.handle)
        .await.unwrap().unwrap();
}

fn assert_reaped(program: &LinkedProgram, host: &ValidationHost) {
    assert!(program.children().is_empty(), "same-program children must be reaped");
    assert!(registry_records(&host.registry).is_empty(), "durable helper ownership must be cleared");
}

#[cfg(debug_assertions)]
#[tokio::test]
async fn output_cap_usage_frame_alone_does_not_confirm_cap_after_cancellation() {
    let program = LinkedProgram::new();
    let mut ready = borg_runtime::native::qualification_hold_provider_after_usage(&program.path);
    let peer = Fixture::start(
        200, serde_json::to_vec(&capped(json!([]))).unwrap(), Duration::ZERO, "",
    ).await;
    let (input, limits) = accepted();
    let host = host(&program, &limits);
    let cfg = config(peer.endpoint.clone());
    let profile = empty_profile();
    let sends = AtomicUsize::new(0);
    let (stop, stopped) = oneshot::channel::<()>();
    let mut running = Box::pin(require_send(execute_loop_step_with_host_inputs_at(
        &input, &cfg, &profile, None, None, None, Some(1), &host,
        async { sends.fetch_add(1, Ordering::SeqCst); Ok(()) },
        async { let _ = stopped.await; },
    )));
    let pid = tokio::select! {
        result = &mut running => panic!("finished before usage READY: {}", result.result.is_ok()),
        pid = &mut ready => pid.unwrap(),
    };
    let records = registry_records(&host.registry);
    assert_eq!(records.len(), 1);
    let record: Value = serde_json::from_slice(&std::fs::read(&records[0]).unwrap()).unwrap();
    assert_eq!(record["pid"], pid);
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).unwrap();
    assert!(!stat.rsplit_once(") ").unwrap().1.starts_with('Z'));
    stop.send(()).unwrap();
    let observation = running.await;
    assert_eq!(observation.request_exposure, RequestExposure::HttpResponseReceived);
    assert_eq!(observation.usage, reported());
    assert_eq!(observation.provider_outcome, ProviderOutcome::Unknown);
    assert_eq!(observation.result.err().as_deref(), Some("native_cancelled"));
    assert_eq!(sends.load(Ordering::SeqCst), 1);
    assert!(!std::path::Path::new(&format!("/proc/{pid}")).exists());
    assert_reaped(&program, &host);
    join_peer(peer, &input).await;
}

#[tokio::test]
async fn output_cap_requires_complete_wire_bounded_payload_and_successful_helper_cleanup() {
    for case in ["empty", "partial_text", "malformed_json", "partial_http", "local_byte_bound"] {
        let program = LinkedProgram::new();
        let (input, limits) = accepted();
        let items = match case {
            "partial_text" => json!([{"type":"message","role":"assistant","status":"incomplete",
                "content":[{"type":"output_text","text":"{\"result\":"}]}]),
            "local_byte_bound" => json!([{"type":"reasoning","summary":[],
                "encrypted_content":"x".repeat(limits.execution.native.context_bytes)}]),
            _ => json!([]),
        };
        let mut wire = serde_json::to_vec(&capped(items.clone())).unwrap();
        if case == "malformed_json" { wire.pop(); }
        let peer = Fixture::start_parts(
            200, wire, Duration::ZERO, "", usize::from(case == "partial_http"),
        ).await;
        let host = host(&program, &limits);
        let profile = empty_profile();
        let cfg = config(peer.endpoint.clone());
        let sends = AtomicUsize::new(0);
        let observation = Box::pin(require_send(execute_loop_step_with_host_inputs_at(
            &input, &cfg, &profile, None, None, None, Some(1), &host,
            async { sends.fetch_add(1, Ordering::SeqCst); Ok(()) },
            std::future::pending(),
        ))).await;
        assert_eq!(observation.request_exposure, RequestExposure::HttpResponseReceived, "{case}");
        let expected = match case {
            "malformed_json" => Some("native_response_malformed"),
            "partial_http" => Some("native_response_incomplete"),
            "local_byte_bound" => Some("native_json_byte_bound_native_context_bytes"),
            _ => None,
        };
        if let Some(expected) = expected {
            assert_eq!(observation.provider_outcome, ProviderOutcome::Unknown, "{case}");
            assert_eq!(observation.usage,
                if case == "local_byte_bound" { reported() } else { UsageObservation::Unknown }, "{case}");
            assert_eq!(observation.result.err().as_deref(), Some(expected), "{case}");
        } else {
            assert_eq!(observation.provider_outcome, ProviderOutcome::OutputCapped, "{case}");
            assert_eq!(observation.usage, reported(), "{case}");
            match observation.result.unwrap() {
                NativeLoopStep::OutputCap(cap) => {
                    assert_eq!(cap.request_index(), 1);
                    assert_eq!(cap.output_items(), &items);
                }
                _ => panic!("{case}: expected typed output-cap continuation"),
            }
        }
        assert_eq!(sends.load(Ordering::SeqCst), 1, "{case}");
        assert_reaped(&program, &host);
        join_peer(peer, &input).await;
    }
}

#[tokio::test]
async fn output_cap_empty_text_does_not_invent_a_started_final_answer() {
    use borg_runtime::native::{NativeFunction,execute_loop_step_after_output_cap_at};
    let program=LinkedProgram::new();let (mut input,limits)=accepted();input.limits.model_requests=2;
    input.resolved_limits=Some(limits.resolve_task(&input).unwrap());
    let profile=NativeLoopProfile {functions:vec![NativeFunction {name:"read_record".into(),description:"Read synthetic data".into(),parameters:json!({"type":"object","properties":{"key":{"type":"integer"}},"required":["key"],"additionalProperties":false})}],web_search:None};
    let host=host(&program,&limits);
    let first=Fixture::start(200,serde_json::to_vec(&capped(json!([{"type":"message","role":"assistant","status":"incomplete","content":[{"type":"output_text","text":""}]}]))).unwrap(),Duration::ZERO,"").await;
    let observed=Box::pin(execute_loop_step_with_host_inputs_at(&input,&config(first.endpoint.clone()),&profile,None,None,None,Some(1),&host,async{Ok(())},std::future::pending())).await;
    let NativeLoopStep::OutputCap(cap)=observed.result.unwrap() else {panic!("cap missing")};join_peer(first,&input).await;
    let next=Fixture::start(200,serde_json::to_vec(&with_usage(json!({"status":"completed","output":[{"type":"function_call","status":"completed","call_id":"next-call","name":"read_record","arguments":"{\"key\":1}"}]}))).unwrap(),Duration::ZERO,"").await;
    let observed=Box::pin(execute_loop_step_after_output_cap_at(&input,&config(next.endpoint.clone()),&profile,&cap,None,None,Some(2),&host.for_request(None),async{Ok(())},std::future::pending())).await;
    assert_eq!(observed.provider_outcome,ProviderOutcome::Completed);assert!(matches!(observed.result,Ok(NativeLoopStep::Call(_))));
    join_peer(next,&input).await;assert_reaped(&program,&host);
}

#[tokio::test]
async fn output_cap_assembly_cannot_bypass_original_credential_filter() {
    use borg_runtime::native::execute_loop_step_after_output_cap_at;
    let program=LinkedProgram::new();let (mut input,limits)=accepted();input.limits.model_requests=2;
    input.resolved_limits=Some(limits.resolve_task(&input).unwrap());let profile=empty_profile();let host=host(&program,&limits);
    // Same public HOME synthetic credential used by the parent fixture, never
    // a provider key. Neither physical response contains the whole value.
    let key=std::env::var("HOME").unwrap();let final_text=serde_json::to_string(&json!({"result":key})).unwrap();
    let midpoint=final_text.char_indices().nth(final_text.chars().count()/2).unwrap().0;
    let (prefix,suffix)=final_text.split_at(midpoint);assert!(!prefix.contains(&key)&&!suffix.contains(&key));
    let first=Fixture::start(200,serde_json::to_vec(&capped(json!([{"type":"message","role":"assistant","status":"incomplete","content":[{"type":"output_text","text":prefix}]}]))).unwrap(),Duration::ZERO,"").await;
    let observed=Box::pin(execute_loop_step_with_host_inputs_at(&input,&config(first.endpoint.clone()),&profile,None,None,None,Some(1),&host,async{Ok(())},std::future::pending())).await;
    let NativeLoopStep::OutputCap(cap)=observed.result.unwrap() else {panic!("cap missing")};join_peer(first,&input).await;
    let next=Fixture::start(200,serde_json::to_vec(&with_usage(envelope(suffix))).unwrap(),Duration::ZERO,"").await;
    let observed=Box::pin(execute_loop_step_after_output_cap_at(&input,&config(next.endpoint.clone()),&profile,&cap,None,None,Some(2),&host.for_request(None),async{Ok(())},std::future::pending())).await;
    assert_eq!(observed.result.err().as_deref(),Some("native_output_contains_credential"));
    assert_eq!(observed.usage,reported());assert_eq!(observed.provider_outcome,ProviderOutcome::Completed);
    join_peer(next,&input).await;assert_reaped(&program,&host);
}
