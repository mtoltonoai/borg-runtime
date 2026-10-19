//! Actual Controller and browser source/head admission. Sources are controlled
//! fixture data, not adopted production charters or provider qualification.
use super::*;
use borg_runtime::{
    charter::*,
    governance::*,
    limits::CharterLimits,
    product::{self, UiRequest},
};
fn cfg(enabled: bool) -> ControllerConfig {
    let mut c = governance::governance_config();
    let p = Arc::make_mut(&mut c.limits);
    p.revision = if enabled {
        "charter-admission-new"
    } else {
        "charter-admission-old"
    }
    .into();
    p.execution.native.schema_bytes = 65_536;
    p.execution.native.json_nodes = 8192;
    if enabled {
        p.charter = Some(CharterLimits {
            record_bytes: 65_536,
            source_bytes: 16_384,
            files: 4,
            clauses: 32,
            metadata_bytes: 8192,
            references: 32,
            reply_items: 32,
            tool_calls: 8,
            reference_depth: 4,
            records: 1024,
            page_items: 4,
            page_bytes: 65_536,
        });
    }
    p.validate().unwrap();
    c
}
fn authored(label: &str) -> DefinitionCharter {
    let sentence = format!("The agent MUST preserve {label}.");
    DefinitionCharter::Inline {
        source: CharterSource {
            files: vec![CharterFile {
                file: "charter.md".into(),
                text: format!("# Purpose\n\n{sentence}\n"),
            }],
            clauses: vec![CharterClauseRef {
                file: "charter.md".into(),
                section: "Purpose".into(),
                sentence,
            }],
        },
    }
}
async fn exchange(c: &Controller, command: Operation) -> Reply {
    let r = Box::pin(c.handle(Request {
        protocol_revision: WIRE_REVISION,
        request_id: "charter-admission-test".into(),
        command: command.clone(),
    }))
    .await;
    r.validate_for(&command).unwrap();
    r
}
async fn selected(c: &Controller, id: &str) -> Definition {
    match call(
        c,
        Operation::GetAgent {
            agent_id: id.into(),
        },
    )
    .await
    {
        Outcome::Ok {
            data: Data::Agent(a),
        } => a.definition,
        x => panic!("agent {x:?}"),
    }
}
fn outcome(r: &Reply) -> CharterDefinitionOutcome {
    match &r.outcome {
        Outcome::Ok {
            data: Data::Charter(value),
        } => match value.as_ref() {
            CharterOutcome::Definition(v) => v.clone(),
            x => panic!("definition result {x:?}"),
        },
        x => panic!("definition result {x:?}"),
    }
}
async fn original(c: &Controller, id: &str) -> OperationRecord {
    match call(
        c,
        Operation::GetOperation {
            operation_id: id.into(),
        },
    )
    .await
    {
        Outcome::Ok {
            data: Data::Operation(v),
        } => v,
        x => panic!("original {x:?}"),
    }
}
async fn audit(c: &Controller, id: &str) -> DecisionRecord {
    match call(
        c,
        Operation::QueryGovernance {
            query: GovernanceQuery::Decision {
                decision: DecisionRef {
                    operation_id: id.into(),
                    point: DecisionPoint::CharterConfiguration,
                },
            },
        },
    )
    .await
    {
        Outcome::Ok {
            data: Data::Governance(v),
        } => match *v {
            GovernanceResult::Decision(v) => v,
            x => panic!("audit {x:?}"),
        },
        x => panic!("audit {x:?}"),
    }
}
fn publish(c: &Controller, id: &str, d: &Definition, charter: DefinitionCharter) -> Operation {
    Operation::MutateCharter {
        operation_id: id.into(),
        mutation: CharterMutation::Publish {
            agent_id: d.id.clone(),
            expected_definition_revision: d.revision.clone(),
            expected_global_revision: c.limits().revision.clone(),
            expected_governance_revision: c.limits().revision.clone(),
            charter,
        },
    }
}
fn choose(c: &Controller, id: &str, d: &Definition, source: CharterSourceRef) -> Operation {
    Operation::MutateCharter {
        operation_id: id.into(),
        mutation: CharterMutation::SelectForFutureTasks {
            agent_id: d.id.clone(),
            expected_definition_revision: d.revision.clone(),
            expected_global_revision: c.limits().revision.clone(),
            expected_governance_revision: c.limits().revision.clone(),
            source,
        },
    }
}
async fn setup(profile: ExecutionProfile) -> (PathBuf, Controller, Task, Definition) {
    let p = path();
    let c = Controller::open_with_config(&p, cfg(false)).unwrap();
    let mut input = native("existing-agent");
    input.definition.execution_profile = Some(profile);
    let old = submit(&c, "historical-task", input).await;
    let definition = selected(&c, &old.input.definition.id).await;
    drop(c);
    controller_owner_released(&p).await;
    let c = Controller::open_with_config(&p, cfg(true)).unwrap();
    assert_eq!(get(&c, &old.id).await.input, old.input);
    (p, c, old, definition)
}
fn plain() -> ExecutionProfile {
    ExecutionProfile::CollaborationV1 { web_search: None }
}
// Publication and future selection retain the original source reference,
// accepted task input and canonical receipts across controller reopen.
//= spec/impl/v2/implementation.md#a-revision-identifies-authoritative-source
//= type=test
//# A charter revision reference MUST resolve to the exact authoritative source of that revision under the applicable access and retention rules.
// Future-head selection identifies the reusable source in the accepted
// definition while preserving the earlier task and its original input.
//= spec/impl/v2/implementation.md#definitions-carry-reusable-charters
//= type=test
//# An agent definition MUST identify its reusable charter source.
#[tokio::test]
async fn charter_publish_select_same_agent_preserves_tasks_and_original_receipts_on_reopen() {
    let (p, c, old, head) = setup(plain()).await;
    let baseline = get(&c, &old.id).await;
    let before = call(
        &c,
        Operation::ListTasks {
            after: None,
            limit: 16,
        },
    )
    .await;
    let op = publish(&c, "publish-original", &head, authored("fixture source"));
    let first = exchange(&c, op.clone()).await;
    let published = outcome(&first);
    assert_eq!(published.effect, CharterDefinitionEffect::PublicationOnly);
    assert_eq!(published.definition.id, head.id);
    assert_eq!(selected(&c, &head.id).await, head);
    assert_eq!(
        call(
            &c,
            Operation::ListTasks {
                after: None,
                limit: 16
            }
        )
        .await,
        before
    );
    let replay = exchange(&c, op.clone()).await;
    assert_eq!(replay.outcome, first.outcome);
    assert_eq!(replay.commit, first.commit);
    let record = original(&c, "publish-original").await;
    assert_eq!(record.intended, serde_json::to_value(&op).unwrap());
    assert_eq!(
        record.outcome,
        Some(serde_json::to_value(&first.outcome).unwrap())
    );
    let decision = audit(&c, "publish-original").await;
    assert_eq!(
        decision.rule,
        RuleRef {
            id: "charter-source-binding".into(),
            revision: "1".into()
        }
    );
    let DecisionDetail::CharterConfiguration(d) = decision.detail else {
        panic!()
    };
    assert_eq!(d.change, EffectiveChange::Widening);
    assert_eq!(d.effect, CharterDefinitionEffect::PublicationOnly);
    assert_eq!(d.source_before, None);
    let select = choose(&c, "select-original", &head, published.source.clone());
    let saved = exchange(&c, select.clone()).await;
    let chosen = outcome(&saved);
    let mut expected = head.clone();
    expected.revision = chosen.definition.revision.clone();
    expected.charter = Some(DefinitionCharter::Reference {
        source: published.source.clone(),
    });
    assert_eq!(chosen.definition, expected);
    assert_eq!(selected(&c, &head.id).await, expected);
    assert_eq!(get(&c, &old.id).await, baseline);
    let mut task_input = native(&head.id);
    task_input.definition = expected.clone();
    let future = submit(&c, "future-task", task_input).await;
    let source = call(
        &c,
        Operation::QueryCharter {
            query: CharterQuery::Session {
                session_id: future.session_id.clone(),
            },
        },
    )
    .await;
    assert!(
        matches!(&source,Outcome::Ok{data:Data::CharterQuery(value)} if matches!(value.as_ref(), CharterQueryResult::Session(Some(e)) if e.charter.reference==published.source&&e.session.initial==published.source&&e.session.pending.is_none()))
    );
    drop(c);
    controller_owner_released(&p).await;
    let c = Controller::open_with_config(&p, cfg(true)).unwrap();
    let replay = exchange(&c, select).await;
    assert_eq!(replay.outcome, saved.outcome);
    assert_eq!(replay.commit, saved.commit);
    assert_eq!(selected(&c, &head.id).await, expected);
    assert_eq!(get(&c, &old.id).await.input, old.input);
    assert_eq!(
        call(
            &c,
            Operation::QueryCharter {
                query: CharterQuery::Session {
                    session_id: future.session_id
                }
            }
        )
        .await,
        source
    );
}
// A fabricated clause sentence and an unresolved source revision are
// refused without changing the selected definition or old task input.
//= spec/impl/v2/implementation.md#every-turn-reply-carries-charter-references
//= type=test
//# A charter clause reference MUST resolve to the clause's source file, section, and exact sentence within the identified charter revision.
#[tokio::test]
async fn charter_publication_refuses_conflicts_and_unresolved_sources_without_head_changes() {
    let (_, c, old, head) = setup(plain()).await;
    let op = publish(&c, "published", &head, authored("original"));
    let first = exchange(&c, op).await;
    let source = outcome(&first).source;
    assert_eq!(
        code(
            exchange(&c, publish(&c, "published", &head, authored("changed")))
                .await
                .outcome
        ),
        "operation_conflict"
    );
    let mut invalid = head.clone();
    invalid.revision = "stale".into();
    assert_eq!(
        code(
            exchange(&c, publish(&c, "stale", &invalid, authored("source")))
                .await
                .outcome
        ),
        "configuration_conflict"
    );
    for (id, global, governance) in [
        ("global", "old-global", c.limits().revision.as_str()),
        ("governance", c.limits().revision.as_str(), "old-selection"),
    ] {
        let command = Operation::MutateCharter {
            operation_id: id.into(),
            mutation: CharterMutation::SelectForFutureTasks {
                agent_id: head.id.clone(),
                expected_definition_revision: head.revision.clone(),
                expected_global_revision: global.into(),
                expected_governance_revision: governance.into(),
                source: source.clone(),
            },
        };
        assert_eq!(
            code(exchange(&c, command).await.outcome),
            "configuration_conflict"
        );
    }
    assert_eq!(
        code(
            exchange(
                &c,
                choose(
                    &c,
                    "absent",
                    &head,
                    CharterSourceRef {
                        definition_id: head.id.clone(),
                        definition_revision: "absent".into()
                    }
                )
            )
            .await
            .outcome
        ),
        "not_found"
    );
    let mut invalid = authored("bad");
    let DefinitionCharter::Inline {
        source: ref mut body,
    } = invalid
    else {
        panic!()
    };
    body.clauses[0].sentence = "The agent MUST invent another sentence.".into();
    assert_eq!(
        code(
            exchange(&c, publish(&c, "invalid-source", &head, invalid))
                .await
                .outcome
        ),
        "invalid_input"
    );
    let mut excessive = authored("large");
    let DefinitionCharter::Inline {
        source: ref mut body,
    } = excessive
    else {
        panic!()
    };
    body.files[0]
        .text
        .push_str(&"x".repeat(c.limits().charter.as_ref().unwrap().source_bytes));
    assert_eq!(
        code(
            exchange(&c, publish(&c, "large-source", &head, excessive))
                .await
                .outcome
        ),
        "input_bound"
    );
    assert_eq!(selected(&c, &head.id).await, head);
    assert_eq!(get(&c, &old.id).await.input, old.input);
    assert!(
        serde_json::from_value::<borg_runtime::limits::DefinitionLimitField>(json!("charter"))
            .is_err()
    );
}
#[cfg(debug_assertions)]
#[tokio::test]
async fn charter_publication_commit_failure_leaves_original_identity_retryable_without_partial_source()
 {
    let (p, c, _, head) = setup(plain()).await;
    drop(c);
    controller_owner_released(&p).await;
    let fail = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut config = cfg(true);
    config.qualification_fail_next_commit = Some(fail.clone());
    let c = Controller::open_with_config(&p, config).unwrap();
    let op = publish(&c, "commit-original", &head, authored("rollback"));
    fail.store(true, Ordering::SeqCst);
    let failed = exchange(&c, op.clone()).await;
    assert_eq!(code(failed.outcome), "store_unavailable");
    assert!(!fail.load(Ordering::SeqCst));
    assert!(failed.commit.is_none());
    assert_eq!(selected(&c, &head.id).await, head);
    assert_eq!(
        code(
            call(
                &c,
                Operation::GetOperation {
                    operation_id: "commit-original".into()
                }
            )
            .await
        ),
        "not_found"
    );
    let succeeded = exchange(&c, op).await;
    let value = outcome(&succeeded);
    assert_eq!(value.effect, CharterDefinitionEffect::PublicationOnly);
    assert_eq!(selected(&c, &head.id).await, head);
    audit(&c, "commit-original").await;
}
fn enc(s: &str) -> String {
    s.bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() || b"-._~".contains(&b) {
                (b as char).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect()
}
fn html(s: &str) -> String {
    s.replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&")
}
fn hidden(body: &str, name: &str) -> String {
    let prefix = format!("name=\"{name}\"");
    let element = body
        .split(&prefix)
        .nth(1)
        .unwrap_or_else(|| panic!("missing {name}"))
        .split('>')
        .next()
        .unwrap();
    html(
        element
            .split("value=\"")
            .nth(1)
            .unwrap()
            .split('"')
            .next()
            .unwrap(),
    )
}

async fn browser(c: &Controller, method: &str, path: &str, body: String) -> product::UiResponse {
    Box::pin(product::handle(
        c,
        UiRequest {
            method: method.into(),
            path: path.into(),
            body: body.into_bytes(),
            origin: Some("http://local.invalid".into()),
            expected_origin: "http://local.invalid".into(),
        },
    ))
    .await
}
#[tokio::test]
async fn charter_browser_selected_v1_and_v2_project_exact_definition_and_replay_original_task() {
    for profile in [
        plain(),
        ExecutionProfile::CollaborationV2 { publication: None, web_search: None },
    ] {
        let (_, c, old, head) = setup(profile.clone()).await;
        let published = outcome(
            &exchange(
                &c,
                publish(&c, "ui-publish", &head, authored("browser source")),
            )
            .await,
        );
        let chosen = outcome(&exchange(&c, choose(&c, "ui-select", &head, published.source)).await);
        let page = browser(
            &c,
            "GET",
            &format!("/tasks/new?agent_id={}", enc(&head.id)),
            String::new(),
        )
        .await;
        assert_eq!(page.status, 200, "{}", page.body);
        let selected_charter: DefinitionCharter =
            serde_json::from_str(&hidden(&page.body, "charter")).unwrap();
        assert_eq!(Some(selected_charter), chosen.definition.charter);
        let expected = if matches!(profile, ExecutionProfile::CollaborationV1 { .. }) {
            "collaboration_v1"
        } else {
            "collaboration_v2"
        };
        assert_eq!(hidden(&page.body, "execution_profile"), expected);
        let mut fields = vec![
            ("operation_id", hidden(&page.body, "operation_id")),
            ("charter", hidden(&page.body, "charter")),
            ("execution_profile", hidden(&page.body, "execution_profile")),
        ];
        fields.extend([
            ("instruction", "browser requested work".into()),
            ("definition_id", chosen.definition.id.clone()),
            ("definition_revision", chosen.definition.revision.clone()),
            ("executor", "native".into()),
            ("model", SELECTED_MODEL.into()),
            ("reasoning_effort", SELECTED_REASONING_EFFORT.into()),
            ("deadline_unix_ms", future().to_string()),
            ("model_requests", "10".into()),
            ("output_bytes", "16384".into()),
            ("result_bytes", old.input.limits.result_bytes.to_string()),
            ("max_delegation_depth", "1".into()),
            ("acceptance_commands", "[]".into()),
            (
                "output_schema",
                old.input.output_schema.as_ref().unwrap().to_string(),
            ),
        ]);
        let body = fields
            .iter()
            .map(|(k, v)| format!("{}={}", enc(k), enc(v)))
            .collect::<Vec<_>>()
            .join("&");
        let response = browser(&c, "POST", "/tasks", body.clone()).await;
        assert_eq!(response.status, 303, "{}", response.body);
        let record = original(&c, &fields[0].1).await;
        let accepted = task(serde_json::from_value(record.outcome.clone().unwrap()).unwrap());
        assert_eq!(accepted.input.definition, chosen.definition);
        let replay = browser(&c, "POST", "/tasks", body).await;
        assert_eq!(replay.status, 303);
        assert_eq!(original(&c, &fields[0].1).await, record);
        assert_eq!(get(&c, &old.id).await.input, old.input);
    }
}

#[tokio::test]
async fn charter_proposed_receipt_refusal_retains_audit_without_publishing_source() {
    let (p, c, _, head) = setup(plain()).await;
    drop(c);
    controller_owner_released(&p).await;
    let mut config = cfg(true);
    let limits = Arc::make_mut(&mut config.limits);
    limits.revision = "charter-bounded-receipt".into();
    limits.controller.outcome_bytes = 40_000;
    limits.charter.as_mut().unwrap().source_bytes = 36_000;
    limits.validate().unwrap();
    let c = Controller::open_with_config(&p, config.clone()).unwrap();
    let mut body = authored("bounded reply");
    let DefinitionCharter::Inline { source } = &mut body else {
        panic!()
    };
    source.files[0]
        .text
        .push_str(&format!("\n{}", "x".repeat(23_000)));
    let op = publish(&c, "bounded-publication", &head, body);
    let refused = exchange(&c, op.clone()).await;
    assert_eq!(code(refused.outcome.clone()), "capacity");
    assert!(refused.commit.is_some(), "original refusal must be durable");
    assert_eq!(selected(&c, &head.id).await, head);
    let decision = audit(&c, "bounded-publication").await;
    assert!(
        matches!(decision.actual_outcome,ActualDecisionOutcome::MandatoryRefusal{ref code} if code=="capacity")
    );
    let DecisionDetail::CharterConfiguration(detail) = decision.detail else {
        panic!()
    };
    assert_eq!(
        code(
            call(
                &c,
                Operation::QueryCharter {
                    query: CharterQuery::Source {
                        source: detail.proposed.clone()
                    }
                }
            )
            .await
        ),
        "not_found"
    );
    let original = original(&c, "bounded-publication").await;
    drop(c);
    controller_owner_released(&p).await;
    let c = Controller::open_with_config(&p, config).unwrap();
    assert_eq!(exchange(&c, op).await.commit, refused.commit);
    assert_eq!(self::original(&c, "bounded-publication").await, original);
    assert_eq!(selected(&c, &head.id).await, head);
    audit(&c, "bounded-publication").await;
}
fn db_files(p: &std::path::Path) -> Vec<(String, Vec<u8>)> {
    ["", "-wal", "-shm"]
        .into_iter()
        .filter_map(|suffix| {
            let name = format!("{}{suffix}", p.display());
            std::fs::read(&name).ok().map(|b| (name, b))
        })
        .collect()
}
#[tokio::test]
async fn charter_startup_requires_original_publication_selection_and_head_membership() {
    for corruption in [
        "published-head",
        "deleted-definition",
        "deleted-audit",
        "selected-head-rollback",
    ] {
        let (p, c, _, head) = setup(plain()).await;
        let publication = outcome(
            &exchange(
                &c,
                publish(&c, "membership-publish", &head, authored("membership")),
            )
            .await,
        );
        let selected_result = if corruption == "selected-head-rollback" {
            Some(outcome(
                &exchange(
                    &c,
                    choose(&c, "membership-select", &head, publication.source.clone()),
                )
                .await,
            ))
        } else {
            None
        };
        drop(c);
        controller_owner_released(&p).await;
        // First prove that the exact valid retained state can reopen.
        let c = Controller::open_with_config(&p, cfg(true)).unwrap();
        audit(&c, "membership-publish").await;
        drop(c);
        controller_owner_released(&p).await;
        let db = rusqlite::Connection::open(&p).unwrap();
        match corruption {
            "published-head" => {
                db.execute(
                    "UPDATE agents SET definition=?2 WHERE id=?1",
                    rusqlite::params![
                        head.id,
                        serde_json::to_string(&publication.definition).unwrap()
                    ],
                )
                .unwrap();
            }
            "deleted-definition" => {
                db.execute(
                    "DELETE FROM definitions WHERE id=?1 AND revision=?2",
                    rusqlite::params![head.id, publication.definition.revision],
                )
                .unwrap();
            }
            "deleted-audit" => {
                db.execute(
                    "UPDATE receipts SET governance=NULL WHERE id='membership-publish'",
                    [],
                )
                .unwrap();
            }
            "selected-head-rollback" => {
                assert!(selected_result.is_some());
                db.execute(
                    "UPDATE agents SET definition=?2 WHERE id=?1",
                    rusqlite::params![head.id, serde_json::to_string(&head).unwrap()],
                )
                .unwrap();
            }
            _ => unreachable!(),
        }
        drop(db);
        let before = db_files(&p);
        assert!(
            Controller::open_with_config(&p, cfg(true)).is_err(),
            "{corruption}"
        );
        assert_eq!(
            db_files(&p),
            before,
            "startup must retain original corrupt bytes: {corruption}"
        );
    }
}
#[tokio::test]
async fn charter_same_source_is_neutral_and_later_numeric_head_keeps_original_sessions() {
    let (p, c, _, head) = setup(plain()).await;
    let published = outcome(
        &exchange(
            &c,
            publish(&c, "neutral-publish", &head, authored("neutral")),
        )
        .await,
    );
    let chosen = outcome(
        &exchange(
            &c,
            choose(&c, "first-select", &head, published.source.clone()),
        )
        .await,
    );
    let mut input = native(&head.id);
    input.definition = chosen.definition.clone();
    let accepted = submit(&c, "accepted-with-source", input).await;
    let prior = call(
        &c,
        Operation::QueryCharter {
            query: CharterQuery::Session {
                session_id: accepted.session_id.clone(),
            },
        },
    )
    .await;
    let same = outcome(
        &exchange(
            &c,
            choose(
                &c,
                "same-source",
                &chosen.definition,
                published.source.clone(),
            ),
        )
        .await,
    );
    let DecisionDetail::CharterConfiguration(detail) = audit(&c, "same-source").await.detail else {
        panic!()
    };
    assert_eq!(detail.change, EffectiveChange::Neutral);
    let op = Operation::UpdateDefinition {
        operation_id: "later-numeric".into(),
        update: DefinitionUpdate {
            agent_id: head.id.clone(),
            expected_revision: same.definition.revision.clone(),
            expected_global_revision: c.limits().revision.clone(),
            expected_policy_revision: c
                .limits()
                .definition_updates
                .as_ref()
                .unwrap()
                .revision
                .clone(),
            changes: std::collections::BTreeMap::from([(
                borg_runtime::limits::DefinitionLimitField::NativeContextBytes,
                c.limits().execution.native.context_bytes,
            )]),
        },
    };
    let result = exchange(&c, op).await;
    let Outcome::Ok {
        data: Data::DefinitionUpdate(next),
    } = result.outcome
    else {
        panic!("numeric {result:?}")
    };
    assert_eq!(next.selected_definition.charter, same.definition.charter);
    assert_eq!(get(&c, &accepted.id).await.input, accepted.input);
    assert_eq!(
        call(
            &c,
            Operation::QueryCharter {
                query: CharterQuery::Session {
                    session_id: accepted.session_id.clone()
                }
            }
        )
        .await,
        prior
    );
    drop(c);
    controller_owner_released(&p).await;
    let c = Controller::open_with_config(&p, cfg(true)).unwrap();
    assert_eq!(selected(&c, &head.id).await, next.selected_definition);
    assert_eq!(get(&c, &accepted.id).await.input, accepted.input);
}
#[tokio::test]
async fn charter_disabled_governance_and_conflicted_browser_form_cannot_apply() {
    let (p, c, _, head) = setup(plain()).await;
    drop(c);
    controller_owner_released(&p).await;
    let mut config = cfg(true);
    Arc::make_mut(&mut config.limits).governance = None;
    let c = Controller::open_with_config(&p, config).unwrap();
    assert_eq!(
        code(
            exchange(&c, publish(&c, "disabled", &head, authored("disabled")))
                .await
                .outcome
        ),
        "governance_disabled"
    );
    assert_eq!(selected(&c, &head.id).await, head);
    let saved = serde_json::to_string(&authored("conflict")).unwrap();
    let fields = [
        ("operation_id", "conflicted-ui"),
        ("executor", "native"),
        ("model", SELECTED_MODEL),
        ("reasoning_effort", SELECTED_REASONING_EFFORT),
        ("execution_profile", "collaboration_v1"),
        ("charter", saved.as_str()),
        ("charter_definition_id", "other-agent"),
        ("charter_definition_revision", "other-revision"),
    ];
    let body = fields
        .iter()
        .map(|(k, v)| format!("{}={}", enc(k), enc(v)))
        .collect::<Vec<_>>()
        .join("&");
    let refused = browser(&c, "POST", "/tasks", body).await;
    assert_eq!(refused.status, 400);
    assert_eq!(hidden(&refused.body, "charter"), saved);
    assert_eq!(
        hidden(&refused.body, "charter_definition_id"),
        "other-agent"
    );
    assert_eq!(
        hidden(&refused.body, "charter_definition_revision"),
        "other-revision"
    );
    assert_eq!(hidden(&refused.body, "operation_id"), "conflicted-ui");
    assert_eq!(
        code(
            call(
                &c,
                Operation::GetOperation {
                    operation_id: "conflicted-ui".into()
                }
            )
            .await
        ),
        "not_found"
    );
}
