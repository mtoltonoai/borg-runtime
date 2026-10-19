//! Controller/codec qualification only. Controlled legacy JSON is a representation
//! fixture, not a completed schema migration or a worker/provider qualification.
use super::*;

async fn reply(c: &Controller, command: Operation) -> Reply {
    c.handle(Request {
        protocol_revision: WIRE_REVISION,
        request_id: "inference-test".into(),
        command,
    })
    .await
}

#[test]
fn inference_codec_preserves_absence_and_rejects_open_or_partial_settings() {
    let old = r#"{"id":"d","revision":"1","executor":"native"}"#;
    let definition: Definition = serde_json::from_str(old).unwrap();
    assert_eq!(definition.inference, None);
    assert_eq!(definition.execution_profile, None);
    assert_eq!(
        definition.required_inference(),
        Err("template_inference_unbound")
    );
    assert_eq!(serde_json::to_string(&definition).unwrap(), old);
    let mut selected = definition.clone();
    selected.inference = Some(InferenceSettings::selected());
    assert_eq!(selected.required_inference().unwrap().model, "gpt-6-astra");
    assert_eq!(
        selected.required_inference().unwrap().reasoning_effort,
        "max"
    );
    for malformed in [
        json!({"model":"gpt-6-astra"}),
        json!({"reasoning_effort":"max"}),
        json!({"model":"gpt-6-astra","reasoning_effort":"max","provider":"other"}),
        json!({"model":false,"reasoning_effort":"max"}),
    ] {
        let mut value = serde_json::to_value(&definition).unwrap();
        value["inference"] = malformed;
        assert!(serde_json::from_value::<Definition>(value).is_err());
    }
    assert!(
        serde_json::from_str::<InferenceSettings>(
            r#"{"model":"gpt-6-astra","model":"other","reasoning_effort":"max"}"#
        )
        .is_err()
    );
    for (model, effort) in [("gpt-6.1-sol", "max"), ("gpt-6-astra", "high")] {
        selected.inference = Some(InferenceSettings {
            model: model.into(),
            reasoning_effort: effort.into(),
        });
        assert_eq!(
            selected.required_inference(),
            Err("template_inference_unsupported")
        );
    }
}

#[test]
fn inference_profile_has_one_closed_definition_authority() {
    let input = configured_native("single-profile-authority");
    let profile = json!({"kind":"collaboration_v1"});
    let mut value = serde_json::to_value(&input).unwrap();
    value["execution_profile"] = profile.clone();
    assert!(serde_json::from_value::<TaskInput>(value).is_err());
    let mut value = serde_json::to_value(&input).unwrap();
    value["native_workflow"]["child"]["execution_profile"] = profile.clone();
    assert!(serde_json::from_value::<TaskInput>(value).is_err());
    let mut value = serde_json::to_value(&input).unwrap();
    value["definition"]["execution_profile"] = profile;
    let decoded: TaskInput = serde_json::from_value(value).unwrap();
    assert_eq!(
        decoded.definition.execution_profile,
        Some(ExecutionProfile::CollaborationV1 { web_search: None })
    );
    for malformed in [
        json!({"kind":"collaboration_v1","other":true}),
        json!({"kind":"collaboration_v1","web_search":{"max_tool_calls":1,"other":true}}),
        json!({"kind":"unknown"}),
    ] {
        let mut value = serde_json::to_value(&input).unwrap();
        value["definition"]["execution_profile"] = malformed;
        assert!(serde_json::from_value::<TaskInput>(value).is_err());
    }
}

#[tokio::test]
async fn inference_profile_is_immutable_per_revision_across_dispatch_and_restart() {
    let p = path();
    let c = Controller::open_with_config(&p, config()).unwrap();
    let mut first_input = native("profile-instance");
    first_input.definition.execution_profile =
        Some(ExecutionProfile::CollaborationV1 { web_search: None });
    let first = submit(&c, "profile-original", first_input.clone()).await;
    let mut refusals = Vec::new();
    for (label, profile) in [(
        "search",
        Some(ExecutionProfile::CollaborationV1 {
            web_search: Some(NativeSearchProfile { max_tool_calls: 2 }),
        }),
    )] {
        let mut changed = first_input.clone();
        changed.definition.execution_profile = profile;
        let op = Operation::SubmitTask {
            operation_id: format!("profile-conflict-{label}"),
            input: (changed).into(),
        };
        let result = reply(&c, op.clone()).await;
        assert_eq!(code(result.outcome.clone()), "definition_conflict");
        assert!(result.commit.is_some());
        refusals.push((op, result));
    }
    let mut later_input = first_input.clone();
    later_input.definition.revision = "profile-revision-2".into();
    later_input.definition.execution_profile = Some(ExecutionProfile::CollaborationV1 {
        web_search: Some(NativeSearchProfile { max_tool_calls: 2 }),
    });
    let later = submit(&c, "profile-later-revision", later_input.clone()).await;
    let w = worker("profile-worker", "one", 1, 0);
    register(&c, &w).await;
    let delivered = commands(&c, &w, vec![]).await;
    assert!(
        matches!(&delivered[0], WorkerCommand::RunAttempt { task, .. }
        if task.definition == first_input.definition)
    );
    drop(c);
    let c = Controller::open_with_config(&p, config()).unwrap();
    assert_eq!(get(&c, &first.id).await.input, accepted_input(&c, &first_input));
    assert_eq!(get(&c, &later.id).await.input, accepted_input(&c, &later_input));
    assert_eq!(commands(&c, &w, vec![]).await, delivered);
    for (op, original) in refusals {
        let replay = reply(&c, op).await;
        assert_eq!(replay.outcome, original.outcome);
        assert_eq!(replay.commit, original.commit);
    }
}

#[tokio::test]
async fn inference_child_profile_is_its_own_definition_without_inheritance() {
    let c = Controller::open_with_config(path(), config()).unwrap();
    let mut input = configured_native("profile-parent");
    input.definition.execution_profile = Some(ExecutionProfile::CollaborationV1 {
        web_search: Some(NativeSearchProfile { max_tool_calls: 2 }),
    });
    let original_child = configured_template_mut(&mut input).definition.clone();
    assert_eq!(
        original_child.execution_profile,
        Some(ExecutionProfile::CollaborationV1 { web_search: None })
    );
    let accepted = submit(&c, "own-child-profile", input.clone()).await;
    let child = borg_runtime::delegation::coding_child(&accepted.input, "bounded").unwrap();
    assert_eq!(child.definition, original_child);
    // The internal child carries the parent's accepted metadata binding.
    // An external declaration supplies an unresolved input, then receives its
    // own controller binding; it cannot submit that trusted internal snapshot.
    assert!(child.resolved_limits.is_some());
    let mut external_child = child.clone();
    external_child.resolved_limits = None;
    external_child.repository.as_mut().unwrap().url.clear();
    external_child.repository.as_mut().unwrap().checkpoint_remote.clear();
    let declared = submit(&c, "declared-child-definition", external_child).await;
    assert_eq!(declared.input.as_ref(), &child);
    let mut changed = input.clone();
    configured_template_mut(&mut changed)
        .definition
        .execution_profile = None;
    assert_eq!(
        code(
            call(
                &c,
                Operation::SubmitTask {
                    operation_id: "child-profile-conflict".into(),
                    input: (changed.clone()).into()
                }
            )
            .await
        ),
        "template_profile_unbound"
    );
    configured_template_mut(&mut changed).definition.revision = "child-profile-2".into();
    configured_template_mut(&mut changed)
        .definition
        .execution_profile = Some(ExecutionProfile::CollaborationV1 { web_search: None });
    let accepted = submit(&c, "new-child-profile-revision", changed).await;
    let child = borg_runtime::delegation::coding_child(&accepted.input, "bounded").unwrap();
    assert_eq!(
        child.definition.execution_profile,
        Some(ExecutionProfile::CollaborationV1 { web_search: None })
    );
}

// Traceability scope: Both executors reject absent or unsupported model/effort bindings, leave the task list empty, and replay the same committed refusals after restart.
//= spec/impl/v2/implementation.md#definitions-select-execution
//= type=test
//# An agent definition MUST specify the model used by its executor.
//= spec/impl/v2/implementation.md#definitions-select-execution
//= type=test
//# An agent definition MUST specify the reasoning effort used by its executor.
#[tokio::test]
async fn inference_new_submissions_require_explicit_selection_with_immutable_refusal() {
    let p = path();
    let c = Controller::open_with_config(&p, config()).unwrap();
    register_repository_fixture(&c, &fixture_repository_registration()).await;
    let mut recorded = Vec::new();
    for kind in [ExecutorKind::Native, ExecutorKind::Coding] {
        for mode in ["absent", "model", "effort", "profile"] {
            let mut input = if kind == ExecutorKind::Native {
                native("n")
            } else {
                coding("c")
            };
            match mode {
                "absent" => input.definition.inference = None,
                "model" => {
                    input.definition.inference.as_mut().unwrap().model = "gpt-6.1-sol".into()
                }
                "profile" => input.definition.execution_profile = None,
                _ => {
                    input
                        .definition
                        .inference
                        .as_mut()
                        .unwrap()
                        .reasoning_effort = "high".into()
                }
            }
            let op = Operation::SubmitTask {
                operation_id: format!("refusal-{kind:?}-{mode}"),
                input: (input).into(),
            };
            let first = reply(&c, op.clone()).await;
            assert_eq!(
                code(first.outcome.clone()),
                if mode == "absent" {
                    "template_inference_unbound"
                } else if mode == "profile" {
                    "template_profile_unbound"
                } else {
                    "template_inference_unsupported"
                }
            );
            assert!(first.commit.is_some());
            recorded.push((op, first));
        }
    }
    assert!(
        matches!(call(&c, Operation::ListTasks { after: None, limit: 10 }).await, Outcome::Ok { data: Data::Tasks(v) } if v.is_empty())
    );
    drop(c);
    let c = Controller::open_with_config(&p, config()).unwrap();
    for (op, first) in recorded {
        let again = reply(&c, op).await;
        assert_eq!(again.outcome, first.outcome);
        assert_eq!(again.commit, first.commit);
    }
}

// Traceability scope: A later definition revision does not alter the first accepted input, dispatched definition or replayed command after controller restart.
//= spec/impl/v2/implementation.md#definitions-select-execution
//= type=test
//# An execution attempt MUST use the inference settings of its accepted definition revision.
#[tokio::test]
async fn inference_task_and_dispatched_attempt_keep_their_accepted_revision() {
    let p = path();
    let c = Controller::open_with_config(&p, config()).unwrap();
    let first_input = coding("reused-instance");
    let first = submit(&c, "first-explicit", first_input.clone()).await;
    let mut later_input = first_input.clone();
    later_input.definition.revision = "revision-2".into();
    let later = submit(&c, "later-explicit", later_input.clone()).await;
    assert_eq!(get(&c, &first.id).await.input, accepted_input(&c, &first_input));
    assert_eq!(get(&c, &later.id).await.input, accepted_input(&c, &later_input));
    let w = worker("selected-worker", "one", 0, 1);
    register(&c, &w).await;
    let delivered = commands(&c, &w, vec![]).await;
    assert!(
        matches!(&delivered[0], WorkerCommand::RunAttempt { task, .. } if task.definition == first_input.definition)
    );
    assert_eq!(
        get(&c, &first.id).await.input.definition.inference,
        Some(InferenceSettings::selected())
    );
    drop(c);
    let c = Controller::open_with_config(&p, config()).unwrap();
    assert_eq!(get(&c, &first.id).await.input, accepted_input(&c, &first_input));
    assert_eq!(get(&c, &later.id).await.input, accepted_input(&c, &later_input));
    assert_eq!(commands(&c, &w, vec![]).await, delivered);
}

/// Produce the exact earlier JSON shape by removing only the new explicit field
/// from a closed, consistent fixture. This is not a production migration helper.
fn legacy_shape(path: &std::path::Path, remove_inference: bool) {
    let c = rusqlite::Connection::open(path).unwrap();
    let field = r#""inference":{"model":"gpt-6-astra","reasoning_effort":"max"},"#;
    let profile = r#""execution_profile":{"kind":"collaboration_v1"},"#;
    for (table, column) in [
        ("tasks", "body"),
        ("sessions", "body"),
        ("definitions", "body"),
        ("agents", "definition"),
        ("receipts", "request"),
        ("receipts", "outcome"),
        ("history", "body"),
    ] {
        if remove_inference {
            c.execute(
                &format!("UPDATE {table} SET {column}=replace({column},?1,'')"),
                [field],
            )
            .unwrap();
        }
        c.execute(
            &format!("UPDATE {table} SET {column}=replace({column},?1,'')"),
            [profile],
        )
        .unwrap();
    }
}
#[tokio::test]
async fn inference_unbound_stored_definition_refuses_without_rewrite() {
    let p=path();let c=Controller::open_with_config(&p,config()).unwrap();
    submit(&c,"original-bound",coding("bound-agent")).await;
    drop(c);controller_owner_released(&p).await;
    legacy_shape(&p,true);
    let before=std::fs::read(&p).unwrap();
    assert!(Controller::open_with_config(&p,config()).err().unwrap().contains("corrupt"));
    assert_eq!(std::fs::read(&p).unwrap(),before,"refusal must not backfill or rewrite unsupported definition");
}

#[tokio::test]
async fn inference_configured_child_requires_its_own_explicit_definition_binding() {
    let c = Controller::open_with_config(path(), config()).unwrap();
    register_repository_fixture(&c, &fixture_repository_registration()).await;
    for mode in ["absent", "wrong-effort", "profile"] {
        let mut input = configured_native("inference-parent");
        let child = configured_template_mut(&mut input);
        if mode == "absent" {
            child.definition.inference = None;
        } else if mode == "profile" {
            child.definition.execution_profile = None;
        } else {
            child
                .definition
                .inference
                .as_mut()
                .unwrap()
                .reasoning_effort = "high".into();
        }
        let refused = call(
            &c,
            Operation::SubmitTask {
                operation_id: format!("child-{mode}"),
                input: (input).into(),
            },
        )
        .await;
        assert_eq!(
            code(refused),
            if mode == "absent" {
                "template_inference_unbound"
            } else if mode == "profile" {
                "template_profile_unbound"
            } else {
                "template_inference_unsupported"
            }
        );
    }
    let input = configured_native("selected-parent");
    let expected = configured_template_mut(&mut input.clone())
        .definition
        .clone();
    let parent = submit(&c, "selected-parent-submit", input).await;
    let child =
        borg_runtime::delegation::coding_child(&parent.input, "bounded instruction").unwrap();
    assert_eq!(child.definition, expected);
    assert_eq!(
        child.definition.required_inference().unwrap(),
        &InferenceSettings::selected()
    );
}

#[tokio::test]
// Traceability scope: Typed native association and coding-plus-read ambiguity admission refusal; no malformed duplicate-field wire claim.
//= spec/impl/v2/implementation.md#repository-associations-are-versioned-metadata
//= type=test
//# A task MUST have at most one primary repository association.
async fn inference_repository_read_association_is_explicit_read_only_and_immutable() {
    let p = path();
    let c = Controller::open_with_config(&p, config()).unwrap();
    let mut registration = fixture_repository_registration();
    registration.repository_id = "project-source".into();
    registration.source_endpoint = "project-source".into();
    register_repository_fixture(&c, &registration).await;
    register_repository_fixture(&c, &fixture_repository_registration()).await;
    let mut input = native("repo-native");
    input.repository_read = Some(RepositoryReadSource {
        registration: Some(registration.binding()),
        repository: String::new(),
        start_commit: "a".repeat(40),
    });
    let accepted = submit(&c, "repo-native", input.clone()).await;
    let mut expected = accepted_input(&c, &input);
    project_repository_fixture(&mut expected, &registration);
    assert_eq!(accepted.input, expected);
    assert!(accepted.input.repository.is_none());
    assert!(accepted.input.acceptance_commands.is_empty());
    for mode in ["bad-commit", "coding-ambiguous", "control-identity"] {
        let mut invalid = if mode == "coding-ambiguous" {
            coding("coding-ambiguous")
        } else {
            native(mode)
        };
        invalid.repository_read = input.repository_read.clone();
        if mode == "bad-commit" {
            invalid.repository_read.as_mut().unwrap().start_commit = "main".into();
        }
        if mode == "control-identity" {
            invalid.repository_read.as_mut().unwrap().repository = "bad\nidentity".into();
        }
        assert_eq!(
            code(
                call(
                    &c,
                    Operation::SubmitTask {
                        operation_id: format!("read-{mode}"),
                        input: (invalid).into()
                    }
                )
                .await
            ),
            "invalid_input"
        );
    }
    let mut credential = native("credential-source");
    credential.repository_read = Some(RepositoryReadSource {
        registration: Some(registration.binding()),
        repository: "https://user:synthetic-password@example.invalid/repo".into(),
        start_commit: "b".repeat(40),
    });
    let refused = reply(
        &c,
        Operation::SubmitTask {
            operation_id: "credential-read-source".into(),
            input: (credential).into(),
        },
    )
    .await;
    assert!(matches!(refused.outcome, Outcome::Refused { .. }));
    assert!(
        refused.commit.is_none(),
        "known credential source must never enter durable refusal storage"
    );
    drop(c);
    let bytes = std::fs::read(&p).unwrap();
    assert!(
        !bytes
            .windows(b"synthetic-password".len())
            .any(|x| x == b"synthetic-password")
    );
    let c = Controller::open_with_config(&p, config()).unwrap();
    assert_eq!(get(&c, &accepted.id).await.input, expected);
    let plain = native("plain");
    assert!(
        !serde_json::to_string(&plain)
            .unwrap()
            .contains("repository_read")
    );
    let child =
        borg_runtime::delegation::coding_child(&accepted_input(&c, &configured_native("no-inferred-read")), "bounded")
            .unwrap();
    assert!(child.repository_read.is_none());
}

#[tokio::test]
async fn inference_unbound_stored_profile_refuses_without_rewrite() {
    let p=path();let c=Controller::open_with_config(&p,config()).unwrap();
    submit(&c,"original-bound",coding("bound-agent")).await;
    drop(c);controller_owner_released(&p).await;
    legacy_shape(&p,false);
    let before=std::fs::read(&p).unwrap();
    assert!(Controller::open_with_config(&p,config()).err().unwrap().contains("corrupt"));
    assert_eq!(std::fs::read(&p).unwrap(),before,"refusal must not backfill or rewrite unsupported definition");
}

#[tokio::test]
async fn profile_semantics_refuse_before_new_task_and_replay_the_refusal() {
    let p = path();
    let c = Controller::open_with_config(&p, config()).unwrap();
    register_repository_fixture(&c, &fixture_repository_registration()).await;
    let mut cases = Vec::new();
    let mut native_input = native("zero-search");
    native_input.definition.execution_profile = Some(ExecutionProfile::CollaborationV1 {
        web_search: Some(NativeSearchProfile { max_tool_calls: 0 }),
    });
    cases.push(("zero-search", native_input, "native_web_search_limit_invalid"));
    let mut coding_input = coding("coding-search");
    coding_input.definition.execution_profile = Some(ExecutionProfile::CollaborationV1 {
        web_search: Some(NativeSearchProfile { max_tool_calls: 1 }),
    });
    cases.push(("coding-search", coding_input, "coding_profile_has_native_provider_settings"));
    let mut child_input = configured_native("child-search");
    configured_template_mut(&mut child_input).definition.execution_profile =
        Some(ExecutionProfile::CollaborationV1 {
            web_search: Some(NativeSearchProfile { max_tool_calls: 1 }),
        });
    cases.push(("child-search", child_input, "coding_profile_has_native_provider_settings"));
    let mut refusals = Vec::new();
    for (id, input, reason) in cases {
        let op = Operation::SubmitTask { operation_id: id.into(), input: (input).into() };
        let first = reply(&c, op.clone()).await;
        assert_eq!(code(first.outcome.clone()), "invalid_input");
        assert!(format!("{:?}", first.outcome).contains(reason));
        assert!(first.commit.is_some());
        refusals.push((op, first));
    }
    assert!(matches!(call(&c, Operation::ListTasks { after: None, limit: 10 }).await,
        Outcome::Ok { data: Data::Tasks(v) } if v.is_empty()));
    drop(c);
    let c = Controller::open_with_config(&p, config()).unwrap();
    for (op, first) in refusals {
        let replay = reply(&c, op).await;
        assert_eq!(replay.outcome, first.outcome);
        assert_eq!(replay.commit, first.commit);
    }
}
