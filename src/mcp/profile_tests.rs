//! Profile/catalogue contracts only; actual actor/domain mutations remain the
//! controller integration tests, not proof supplied by these schema fixtures.
use super::*;
use std::collections::BTreeSet;

const ADDED: [&str; 31] = [
    "collab_question_pose", "collab_question_answer", "collab_question_cancel",
    "collab_question_supersede", "collab_answer_replace", "collab_answer_withdraw",
    "collab_answer_mark_moot", "collab_question_get", "collab_questions_list",
    "collab_question_answers", "collab_question_actions", "collab_decisions_list",
    "collab_document_create", "collab_document_version_create", "collab_document_owner_set",
    "collab_document_get", "collab_documents_list", "collab_document_versions_list",
    "collab_comment_post", "collab_comments_list", "collab_review_create",
    "collab_review_get", "collab_reviews_list", "collab_review_target_get",
    "collab_review_decide", "collab_review_withdraw", "collab_task_coordination_get",
    "collab_task_waits", "collab_task_wait", "collab_task_unwait", "collab_task_owner",
];
fn v1() -> ExecutionProfile { ExecutionProfile::CollaborationV1 { web_search: None } }
fn v2() -> ExecutionProfile { ExecutionProfile::CollaborationV2 { publication: None, web_search: None } }

#[test]
fn v1_bytes_and_absent_definition_profile_roundtrip_unchanged() {
    for bytes in [r#"{"kind":"collaboration_v1"}"#,
        r#"{"kind":"collaboration_v1","web_search":{"max_tool_calls":2}}"#] {
        let profile: ExecutionProfile = serde_json::from_str(bytes).unwrap();
        assert_eq!(serde_json::to_string(&profile).unwrap(), bytes);
    }
    let old = r#"{"id":"d","revision":"1","executor":"native"}"#;
    let definition: Definition = serde_json::from_str(old).unwrap();
    assert!(definition.execution_profile.is_none());
    assert_eq!(serde_json::to_string(&definition).unwrap(), old);
}

#[test]
fn v2_selection_is_explicit_closed_and_preserves_existing_search_shape() {
    for bytes in [r#"{"kind":"collaboration_v2"}"#,
        r#"{"kind":"collaboration_v2","web_search":{"max_tool_calls":2}}"#] {
        let profile: ExecutionProfile = serde_json::from_str(bytes).unwrap();
        assert!(matches!(profile, ExecutionProfile::CollaborationV2 { .. }));
        assert_eq!(serde_json::to_string(&profile).unwrap(), bytes);
    }
    for bytes in [r#"{"kind":"collaboration_v2","unknown":true}"#,
        r#"{"kind":"collaboration_v2","kind":"collaboration_v1"}"#,
        r#"{"kind":"collaboration_v2","web_search":{}}"#,
        r#"{"kind":"collaboration_v2","web_search":{"max_tool_calls":1,"other":true}}"#,
        r#"{"kind":"collaboration_v3"}"#] {
        assert!(strict::parse(bytes.as_bytes(), 1024)
            .and_then(|value| serde_json::from_value::<ExecutionProfile>(value).map_err(|e| e.to_string()))
            .is_err(), "{bytes}");
    }
}

#[test]
fn v1_never_gains_workflow_tools_and_v2_keeps_its_exact_prefix() {
    let limits = RuntimeLimits::seed().unwrap();
    assert!(limits.workflow.is_some());
    let legacy = catalogue(&limits);
    assert!(!legacy.is_empty());
    assert_eq!(catalogue_for_profile(&limits, Some(&v1())), legacy);
    assert!(catalogue_for_profile(&limits, None).is_empty());
    assert!(legacy.iter().all(|tool| !ADDED.contains(&tool.name.as_str())));
    let expanded = catalogue_for_profile(&limits, Some(&v2()));
    assert_eq!(expanded.len(), legacy.len() + ADDED.len());
    assert_eq!(&expanded[..legacy.len()], legacy.as_slice());
    let actual: BTreeSet<_> = expanded[legacy.len()..].iter().map(|t| t.name.as_str()).collect();
    assert_eq!(actual, ADDED.into_iter().collect());
    assert_eq!(expanded.iter().map(|tool| &tool.name).collect::<BTreeSet<_>>().len(), expanded.len());
}

#[test]
fn workflow_absence_and_provider_settings_cannot_widen_the_catalogue() {
    let limits = RuntimeLimits::seed().unwrap();
    let search = ExecutionProfile::CollaborationV2 { publication: None,
        web_search: Some(NativeSearchProfile { max_tool_calls: 3 }),
    };
    assert_eq!(catalogue_for_profile(&limits, Some(&search)), catalogue_for_profile(&limits, Some(&v2())));
    let mut absent = limits.clone();
    absent.workflow = None;
    assert_eq!(catalogue_for_profile(&absent, Some(&v2())), catalogue(&absent));
    for name in ADDED {
        assert!(validate_arguments_for_profile(name, &json!({}), &limits, Some(&v1())).is_err());
        assert!(validate_arguments_for_profile(name, &json!({}), &limits, None).is_err());
        assert!(validate_arguments_for_profile(name, &json!({}), &absent, Some(&v2())).is_err());
    }
}

#[test]
fn each_domain_routes_nested_arguments_only_for_v2() {
    let limits = RuntimeLimits::seed().unwrap();
    for (name, arguments) in [
        ("collab_question_pose", json!({"question":{"record":{"kind":"task","task_id":"t"},
            "recipient":{"kind":"agent","agent_id":"a"},"prompt":"Which?",
            "options":[{"id":"yes","label":"Yes","explanation":"Proceed"}],"recommended_option_id":"yes"}})),
        ("collab_document_create", json!({"content":{"title":"A","body":"B"},
            "supplied_content_id":null,"responsible_owner":{"kind":"agent","agent_id":"a"}})),
        ("collab_task_wait", json!({"task_id":"t","expected_revision":1,
            "resolver":{"kind":"question","question_id":"q"}})),
    ] {
        assert!(validate_arguments_for_profile(name, &arguments, &limits, Some(&v2())).is_ok(), "{name}");
        assert!(validate_arguments_for_profile(name, &arguments, &limits, Some(&v1())).is_err(), "{name}");
        assert!(validate_arguments(name, &arguments, &limits).is_err(), "legacy {name}");
        let mut extra = arguments;
        extra["actor"] = json!({"kind":"trusted_host_operator"});
        assert!(validate_arguments_for_profile(name, &extra, &limits, Some(&v2())).is_err(), "{name}");
    }
    // Task coordination begins at revision zero. Only that comment selector
    // permits zero; actual document/review revision checks remain positive.
    assert!(validate_arguments_for_profile("collab_comment_post", &json!({
        "record":{"kind":"task","task_id":"t"},"expected_revision":0,"body":"first"}),
        &limits, Some(&v2())).is_ok());
    assert!(validate_arguments_for_profile("collab_document_version_create", &json!({
        "document_id":"d","expected_revision":0,"content":{"title":"A","body":"B"},
        "supplied_content_id":null}), &limits, Some(&v2())).is_err());
    assert!(validate_arguments_for_profile("collab_review_decide", &json!({
        "review_id":"r","expected_revision":0,"verdict":"approved","explanation":"yes"}),
        &limits, Some(&v2())).is_err());
    let old = json!({"task_id":"t"});
    assert!(validate_arguments_for_profile("collab_task_get", &old, &limits, Some(&v1())).is_ok());
    assert!(validate_arguments_for_profile("collab_task_get", &old, &limits, Some(&v2())).is_ok());
}

#[test]
fn nested_schema_policy_is_not_rewritten_as_flat_identity_limits() {
    let mut limits = RuntimeLimits::seed().unwrap();
    limits.transport.identity_bytes = 13;
    limits.workflow.as_mut().unwrap().text_bytes = 37;
    let tools = catalogue_for_profile(&limits, Some(&v2()));
    let question = tools.iter().find(|t| t.name == "collab_question_pose").unwrap();
    let fields = &question.input_schema["properties"]["question"]["properties"];
    assert_eq!(fields["prompt"]["x-borg-maxUtf8Bytes"], 37);
    assert_eq!(fields["recommended_option_id"]["x-borg-maxUtf8Bytes"], 13);
    let document = tools.iter().find(|t| t.name == "collab_document_create").unwrap();
    assert_eq!(document.input_schema["properties"]["content"]["properties"]["body"]["x-borg-maxUtf8Bytes"], 37);
}

#[test]
fn host_delivery_and_protected_operations_are_not_agent_tools() {
    let limits = RuntimeLimits::seed().unwrap();
    let tools = catalogue_for_profile(&limits, Some(&v2()));
    for name in ["observe_collaboration_delivery", "prepare_collaboration_delivery",
        "begin_collaboration_delivery", "acknowledge_collaboration_delivery",
        "query_workflow_activity", "collab_delivery_ack", "web_search", "collab_rollout"] {
        assert!(!tools.iter().any(|tool| tool.name == name), "{name}");
        assert!(validate_arguments_for_profile(name, &json!({}), &limits, Some(&v2())).is_err(), "{name}");
    }
}
