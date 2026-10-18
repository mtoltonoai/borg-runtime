use super::*;

fn source() -> RepositoryReadSource {
    RepositoryReadSource {
        registration: Some(crate::repository_catalogue::Revision {
            repository_id: "spec".into(),
            registration_revision: "spec-1".into(),
        }),
        repository: "/provisioned/spec.git".into(),
        start_commit: "a".repeat(40),
    }
}

#[test]
fn instruction_source_catalogue_preserves_every_unbound_profile_byte() {
    let limits = RuntimeLimits::seed().unwrap();
    let source = source();
    for profile in [
        ExecutionProfile::CollaborationV1 { web_search: None },
        ExecutionProfile::CollaborationV2 { publication: None, web_search: None },
    ] {
        let old = catalogue_for_profile(&limits, Some(&profile));
        assert_eq!(
            serde_json::to_vec(&catalogue_for_task(&limits, Some(&profile), None)).unwrap(),
            serde_json::to_vec(&old).unwrap()
        );
        let bound = catalogue_for_task(&limits, Some(&profile), Some(&source));
        assert_eq!(bound.len(), old.len());
        let mut changes = 0;
        for (before, after) in old.iter().zip(&bound) {
            if before == after {
                continue;
            }
            assert!(repository_query(&before.name));
            let mut expected = before.clone();
            for (name, property) in expected.input_schema["properties"].as_object_mut().unwrap() {
                if name == "source" || name == "base_source" {
                    assert_eq!(property["enum"], json!(["start", "checkpoint"]));
                    property["enum"] = json!(["start", "checkpoint", "instructions"]);
                }
            }
            assert_eq!(&expected, after);
            changes += 1;
        }
        assert_eq!(changes, 5);
    }
    assert!(catalogue_for_task(&limits, None, Some(&source)).is_empty());
}

#[test]
fn instruction_selector_requires_binding_and_keeps_exact_argument_bounds() {
    let limits = RuntimeLimits::seed().unwrap();
    let profile = ExecutionProfile::CollaborationV1 { web_search: None };
    let source = source();
    let args = json!({"source":"instructions","path":"AGENTS.md","start_line":1,"max_lines":1});
    assert!(
        validate_arguments_for_task(
            "collab_repository_read",
            &args,
            &limits,
            Some(&profile),
            None
        )
        .is_err()
    );
    assert!(
        validate_arguments_for_task(
            "collab_repository_read",
            &args,
            &limits,
            None,
            Some(&source)
        )
        .is_err()
    );
    assert!(
        validate_arguments_for_task(
            "collab_repository_read",
            &args,
            &limits,
            Some(&profile),
            Some(&source)
        )
        .is_ok()
    );
    for (key, value) in [
        ("repository", json!("/other")),
        ("commit", json!("b".repeat(40))),
        ("source_store_key", json!("private")),
    ] {
        let mut changed = args.clone();
        changed[key] = value;
        assert!(
            validate_arguments_for_task(
                "collab_repository_read",
                &changed,
                &limits,
                Some(&profile),
                Some(&source)
            )
            .is_err()
        );
    }
    let mut changed = args.clone();
    changed["max_lines"] = json!(limits.repository.lines_per_page + 1);
    assert!(
        validate_arguments_for_task(
            "collab_repository_read",
            &changed,
            &limits,
            Some(&profile),
            Some(&source)
        )
        .unwrap_err()
        .contains("repository.lines_per_page")
    );
    changed["max_lines"] = json!(limits.repository.lines_per_page);
    assert!(
        validate_arguments_for_task(
            "collab_repository_read",
            &changed,
            &limits,
            Some(&profile),
            Some(&source)
        )
        .is_ok()
    );
    // The additional source is not a workflow capability or registration tool.
    assert!(
        validate_arguments_for_task(
            "collab_task_wait",
            &json!({}),
            &limits,
            Some(&profile),
            Some(&source)
        )
        .is_err()
    );
    assert!(
        validate_arguments_for_task(
            "collab_repository_register",
            &json!({}),
            &limits,
            Some(&profile),
            Some(&source)
        )
        .is_err()
    );
}
