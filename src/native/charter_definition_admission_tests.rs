//! New ordinary publication/selection joined to the existing real helper and
//! native request constructor. No synthetic task is used to publish source.
use super::*;
#[tokio::test]
#[ignore = "requires qualified ordinary helper8 image via BORG_CHARTER_PROFILE_HELPER"]
async fn ordinary_charter_head_selection_reaches_current_native_constructor_for_both_profiles() {
    let (old, new) = policies();
    for (label, selected_profile) in [
        ("v1", ExecutionProfile::CollaborationV1 { web_search: None }),
        ("v2", ExecutionProfile::CollaborationV2 { publication: None, web_search: None }),
    ] {
        // Existing head uses the selected global capacities before charter
        // activation. This does not upgrade a historical V1 capability set.
        let mut before = new.clone();
        before.revision = format!("charter-head-before-{label}");
        before.charter = None;
        let dir = root(&format!("charter-head-{label}"));
        let db = dir.join("state.sqlite");
        let c = open(&db, &before);
        let mut initial = input("same-agent", &before);
        initial.definition.execution_profile = Some(selected_profile.clone());
        let historical = submit(&c, "original-task", initial.clone()).await;
        let historical = get(&c, &historical.id).await;
        drop(c);
        release(&db).await;
        let c = open(&db, &new);
        let publish = Operation::MutateCharter {
            operation_id: "ordinary-publish".into(),
            mutation: CharterMutation::Publish {
                agent_id: initial.definition.id.clone(),
                expected_definition_revision: initial.definition.revision.clone(),
                expected_global_revision: new.revision.clone(),
                expected_governance_revision: new.revision.clone(),
                charter: DefinitionCharter::Inline {
                    source: maximum_source(&new),
                },
            },
        };
        let published = call(&c, publish).await;
        let Outcome::Ok {
            data: Data::Charter(CharterOutcome::Definition(published)),
        } = published.outcome
        else {
            panic!("publish: {published:?}")
        };
        let selected = call(
            &c,
            Operation::MutateCharter {
                operation_id: "ordinary-select".into(),
                mutation: CharterMutation::SelectForFutureTasks {
                    agent_id: initial.definition.id.clone(),
                    expected_definition_revision: initial.definition.revision.clone(),
                    expected_global_revision: new.revision.clone(),
                    expected_governance_revision: new.revision.clone(),
                    source: published.source.clone(),
                },
            },
        )
        .await;
        let Outcome::Ok {
            data: Data::Charter(CharterOutcome::Definition(selected)),
        } = selected.outcome
        else {
            panic!("select: {selected:?}")
        };
        let mut next = input("same-agent", &new);
        next.definition = selected.definition.clone();
        let admitted = submit(&c, "future-task", next).await;
        let source = effective(&c, &admitted).await;
        assert_eq!(source.charter.reference, published.source);
        assert_eq!(
            admitted.input.definition.execution_profile,
            Some(selected_profile)
        );
        assert_eq!(get(&c, &historical.id).await, historical);
        let p = profile(&admitted.input);
        super::selected(&p, &new.execution.native).unwrap();
        let request = request_projection(&admitted.input, &p, &source.charter);
        assert!(
            request["input"][0]["content"]
                .as_str()
                .unwrap()
                .contains("Agent MUST keep")
        );
        preflight(
            &admitted.input,
            Some(&source.charter),
            &old,
            &dir.join("native"),
            "native_api_key_unavailable",
            true,
        )
        .await;
        assert_eq!(get(&c, &historical.id).await, historical);
        assert_eq!(effective(&c, &admitted).await, source);
        drop(c);
        release(&db).await;
    }
}
