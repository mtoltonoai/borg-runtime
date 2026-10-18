//! Actual controller, validator and retained-store acceptance for the combined batch.
#[path = "../identity_support.rs"]
mod support;
use borg_runtime::{charter::*, controller::Controller, limits::*, mcp, protocol::*};
use serde_json::{Value, json};
use std::sync::{Arc, atomic::Ordering};
use support::*;

fn administration(r: Reply) -> IdentityAdministrationOutcome {
    match ok(r) {
        Data::IdentityAdministration(v) => *v,
        v => panic!("identity outcome {v:?}"),
    }
}
#[tokio::test]
async fn authorized_manager_registers_and_replays_exactly() {
    let h = Harness::new("register", "charter", ExecutorKind::Native, true).await;
    let call = invocation(
        &h.caller,
        "collab_agent_register",
        admin_args(
            &h.cfg.limits,
            json!({"definition":definition(&h.cfg.limits,"designer",ExecutorKind::Native,false)}),
        ),
    );
    let first = h.invoke(&call).await;
    let result = administration(first.clone());
    assert!(matches!(result.effect,IdentityEffect::Registered(ref a) if a.id=="designer"));
    assert!(first.commit.is_some());
    assert_eq!(h.invoke(&call).await.outcome, first.outcome);
    let mut conflict = call.clone();
    conflict.arguments["definition"]["id"] = json!("not-created");
    assert_eq!(refused(h.invoke(&conflict).await), "operation_conflict");
    assert_eq!(
        refused(
            api(
                &h.c,
                Operation::GetAgent {
                    agent_id: "not-created".into()
                }
            )
            .await
        ),
        "not_found"
    );
    let mut interrupted = call.clone();
    interrupted.operation_id = id("interrupted");
    interrupted.arguments["definition"]["id"] = json!("commit-interruption");
    h.fail.store(true, Ordering::SeqCst);
    let failure = h.invoke(&interrupted).await;
    assert!(!matches!(failure.outcome, Outcome::Ok { .. }));
    let recovered = h.invoke(&interrupted).await;
    administration(recovered.clone());
    assert_eq!(h.invoke(&interrupted).await.outcome, recovered.outcome);
    let cfg = h.cfg.clone();
    let h = h.reopen(cfg).await;
    assert_eq!(h.invoke(&call).await.outcome, first.outcome);
    assert_eq!(h.invoke(&call).await.commit, first.commit);
    h.close().await;
}
#[tokio::test]
async fn other_agents_cannot_change_any_identity() {
    for agent in ["engineer", "triage", "reviewer"] {
        let h = Harness::new(agent, agent, ExecutorKind::Native, true).await;
        let before = selected(&h.c, "charter").await;
        let admin = admin_args(
            &h.cfg.limits,
            json!({"definition":definition(&h.cfg.limits,"new-id",ExecutorKind::Native,false)}),
        );
        assert_eq!(
            refused(h.call("collab_agent_register", admin).await),
            "identity_manager_required"
        );
        let mut next = before.definition.clone();
        next.revision = "2".into();
        for (tool, fields) in [
            (
                "collab_agent_retire",
                json!({"agent_id":"charter","expected_revision":"1"}),
            ),
            (
                "collab_definition_replace",
                json!({"expected_revision":"1","definition":next}),
            ),
            (
                "collab_charter_publish",
                json!({"agent_id":"charter","expected_definition_revision":"1","charter":charter("new")}),
            ),
            (
                "collab_charter_select",
                json!({"agent_id":"charter","expected_definition_revision":"1","source":{"definition_id":"charter","definition_revision":"1"}}),
            ),
            (
                "collab_charter_set_pending",
                json!({"session_id":h.original.session_id,"expected_revision":1,"target":{"definition_id":"charter","definition_revision":"1"}}),
            ),
        ] {
            assert_eq!(
                refused(h.call(tool, admin_args(&h.cfg.limits, fields)).await),
                "identity_manager_required",
                "{agent}/{tool}"
            )
        }
        assert_eq!(refused(h.call("collab_definition_update",json!({"agent_id":"charter","expected_revision":"1","expected_global_revision":h.cfg.limits.revision,"expected_policy_revision":h.cfg.limits.definition_updates.as_ref().unwrap().revision,"changes":{"native.context_bytes":600000}})).await),"identity_manager_required");
        assert_eq!(selected(&h.c, "charter").await, before);
        h.close().await;
    }
}
#[tokio::test]
async fn implicit_creation_cannot_bypass_manager_policy() {
    let h = Harness::new("implicit", "triage", ExecutorKind::Native, false).await;
    let mut child = h.current().await.input;
    child.resolved_limits = None;
    child.definition.id = "unregistered".into();
    child.limits.model_requests = 1;
    let command = Operation::DelegateTask {
        operation_id: id("delegate"),
        parent_task_id: h.caller.task_id.clone(),
        parent_attempt_id: h.caller.attempt_id.clone(),
        generation: h.caller.generation,
        child: child.clone(),
    };
    assert_eq!(
        refused(api(&h.c, command).await),
        "identity_manager_required"
    );
    child.definition.id = "reviewer".into();
    child.definition.revision = "novel".into();
    assert_eq!(
        refused(
            api(
                &h.c,
                Operation::DelegateTask {
                    operation_id: id("delegate"),
                    parent_task_id: h.caller.task_id.clone(),
                    parent_attempt_id: h.caller.attempt_id.clone(),
                    generation: h.caller.generation,
                    child
                }
            )
            .await
        ),
        "identity_manager_required"
    );
    for field in ["definition", "resolved_limits", "native_workflow"] {
        let mut a = create_args(Some("reviewer"));
        a[field] = json!({});
        assert_eq!(
            refused(h.call("collab_task_create", a).await),
            "invalid_input"
        )
    }
    assert_eq!(h.tasks().await.len(), 1);
    h.close().await;
}
#[tokio::test]
async fn trusted_operator_recovery_remains_distinct() {
    let h = Harness::new("operator", "triage", ExecutorKind::Native, false).await;
    let mut args = admin_args(
        &h.cfg.limits,
        json!({"definition":definition(&h.cfg.limits,"forged",ExecutorKind::Native,false)}),
    );
    args["principal"] = json!("trusted_host_operator");
    assert_eq!(
        refused(h.call("collab_agent_register", args).await),
        "invalid_input"
    );
    ok(api(
        &h.c,
        Operation::RegisterAgent {
            operation_id: id("operator-recovery"),
            definition: definition(
                &h.cfg.limits,
                "operator-recovered",
                ExecutorKind::Native,
                false,
            ),
        },
    )
    .await);
    assert_eq!(
        selected(&h.c, "operator-recovered").await.id,
        "operator-recovered"
    );
    // This ordinary API is explicitly trusted by host/SSH access. A bound MCP
    // check does not contain arbitrary code that can reach this same host API.
    h.close().await;
}
#[tokio::test]
async fn definition_cas_and_complete_validation_precede_commit() {
    let h = Harness::new("full-definition", "charter", ExecutorKind::Native, true).await;
    let old = selected(&h.c, "triage").await.definition;
    let mut replacement = old.clone();
    replacement.revision = "2".into();
    let valid = admin_args(
        &h.cfg.limits,
        json!({"expected_revision":"1","definition":replacement}),
    );
    for key in [
        "expected_revision",
        "expected_global_revision",
        "expected_policy_revision",
        "expected_governance_revision",
    ] {
        let mut a = valid.clone();
        a[key] = json!("stale");
        assert_eq!(
            refused(h.call("collab_definition_replace", a).await),
            "configuration_conflict"
        )
    }
    for field in [
        "inference",
        "execution_profile",
        "limit_overrides",
        "charter",
    ] {
        let mut a = valid.clone();
        a["definition"][field] = match field {
            "inference" => json!({"model":"unconfigured","reasoning_effort":"max"}),
            "execution_profile" => json!({"kind":"invented"}),
            "limit_overrides" => {
                json!({"native":{"function_count":0},"coding":null,"collaboration":null,"memory_notice":null})
            }
            _ => {
                json!({"kind":"reference","source":{"definition_id":"absent","definition_revision":"1"}})
            }
        };
        assert!(matches!(
            h.call("collab_definition_replace", a).await.outcome,
            Outcome::Refused { .. }
        ));
        assert_eq!(selected(&h.c, "triage").await.definition, old);
    }
    let out = administration(h.call("collab_definition_replace", valid).await);
    assert!(matches!(out.effect,IdentityEffect::Replaced(ref a) if a.definition.revision=="2"));
    assert_eq!(h.original.input.definition.revision, "1");
    h.close().await;
}
#[tokio::test]
async fn governance_and_protected_authority_do_not_expand() {
    let mut restricted = policy();
    restricted
        .definition_updates
        .as_mut()
        .unwrap()
        .allowed_fields
        .retain(|field| *field != DefinitionLimitField::NativeContextBytes);
    let h = Harness::with_policy(
        "governance",
        "charter",
        ExecutorKind::Native,
        true,
        restricted,
    )
    .await;
    let before = selected(&h.c, "triage").await.definition;
    for unknown in [false, true] {
        let mut candidate = before.clone();
        candidate.revision = "blocked-full-definition".into();
        if unknown {
            let mut values = h.cfg.limits.execution.memory_notice.clone();
            values.keys += 1;
            candidate.limit_overrides.as_mut().unwrap().memory_notice = Some(values);
        } else {
            let mut values = h.cfg.limits.execution.native.clone();
            values.context_bytes = 600000;
            candidate.limit_overrides.as_mut().unwrap().native = Some(values);
        }
        assert_eq!(
            refused(
                h.call(
                    "collab_definition_replace",
                    admin_args(
                        &h.cfg.limits,
                        json!({"expected_revision":"1","definition":candidate})
                    )
                )
                .await
            ),
            "governance_configuration_blocked"
        );
        assert_eq!(selected(&h.c, "triage").await.definition, before);
    }
    for field in [
        "identity_administration",
        "governance",
        "repository_registrations",
        "credential",
        "protected_release",
        "principal",
    ] {
        let mut a = admin_args(
            &h.cfg.limits,
            json!({"definition":definition(&h.cfg.limits,"forbidden",ExecutorKind::Native,false)}),
        );
        a["definition"][field] = json!({"enabled":true});
        assert_eq!(
            refused(h.call("collab_agent_register", a).await),
            "invalid_input"
        );
    }
    for name in [
        "collab_repository_register",
        "collab_governance_update",
        "collab_release_approve",
    ] {
        assert!(matches!(
            h.call(name, json!({})).await.outcome,
            Outcome::Refused { .. }
        ))
    }
    let mut cfg = h.cfg.clone();
    let p = Arc::make_mut(&mut cfg.limits);
    p.revision = "identity-future-2".into();
    p.identity_administration.as_mut().unwrap().revision = "identity-policy-2".into();
    p.identity_administration
        .as_mut()
        .unwrap()
        .complete_definitions = false;
    let h = h.reopen(cfg).await;
    assert_eq!(
        refused(h.register("refused-by-policy").await),
        "configuration_policy"
    );
    assert_eq!(h.tasks().await.len(), 1);
    h.close().await;
}
#[tokio::test]
async fn retirement_and_future_selection_preserve_history() {
    let h = Harness::new("retirement", "charter", ExecutorKind::Native, true).await;
    let before = h.current().await;
    let published=administration(h.call("collab_charter_publish",admin_args(&h.cfg.limits,json!({"agent_id":"charter","expected_definition_revision":"1","charter":charter("new source")}))).await);
    let IdentityEffect::Charter(CharterOutcome::Definition(publication)) = published.effect else {
        panic!("publication")
    };
    assert_eq!(selected(&h.c, "charter").await.definition.revision, "1");
    administration(h.call("collab_charter_set_pending",admin_args(&h.cfg.limits,json!({"session_id":before.session_id,"expected_revision":1,"target":publication.source}))).await);
    let selected_result=administration(h.call("collab_charter_select",admin_args(&h.cfg.limits,json!({"agent_id":"charter","expected_definition_revision":"1","source":publication.source}))).await);
    let IdentityEffect::Charter(CharterOutcome::Definition(selection)) = selected_result.effect
    else {
        panic!("selection")
    };
    let call = invocation(
        &h.caller,
        "collab_agent_retire",
        admin_args(
            &h.cfg.limits,
            json!({"agent_id":"charter","expected_revision":selection.definition.revision}),
        ),
    );
    let retired = h.invoke(&call).await;
    assert!(administration(retired.clone()).manager_unusable_for_future_tasks);
    assert_eq!(h.current().await.input, before.input);
    assert_eq!(h.current().await.status, before.status);
    assert!(matches!(
        h.register("no-replacement").await.outcome,
        Outcome::Refused { .. }
    ));
    let cfg = h.cfg.clone();
    let h = h.reopen(cfg).await;
    assert_eq!(h.invoke(&call).await.outcome, retired.outcome);
    assert!(selected(&h.c, "charter").await.retired);
    assert_eq!(h.current().await.input, before.input);
    h.close().await;
}
#[tokio::test]
async fn new_catalogue_fits_before_provider_send() {
    let h = Harness::new("catalogue", "charter", ExecutorKind::Native, true).await;
    let mut p = (*h.cfg.limits).clone();
    let old = borg_runtime::mcp::catalogue_for_profile(
        &p,
        Some(&ExecutionProfile::CollaborationV2 { web_search: None }),
    );
    assert_eq!(old.len(), 66);
    p.execution.collaboration.capabilities = h
        .original
        .input
        .execution_limits()
        .unwrap()
        .collaboration
        .capabilities
        .clone();
    let tools = borg_runtime::mcp::catalogue_for_profile(
        &p,
        Some(&ExecutionProfile::CollaborationV2 { web_search: None }),
    );
    assert_eq!(tools.len(), 73);
    assert!(tools.iter().any(|t| t.name == "collab_document_create"));
    assert_eq!(
        tools
            .iter()
            .filter(|t| t.name == "collab_task_create")
            .count(),
        1
    );
    p.execution
        .collaboration
        .capabilities
        .as_mut()
        .unwrap()
        .identity_administration = false;
    assert_eq!(
        borg_runtime::mcp::catalogue_for_profile(
            &p,
            Some(&ExecutionProfile::CollaborationV2 { web_search: None })
        )
        .len(),
        67
    );
    assert_eq!(
        borg_runtime::mcp::catalogue_for_profile(
            &p,
            Some(&ExecutionProfile::CollaborationV1 { web_search: None })
        ),
        borg_runtime::mcp::catalogue(&p)
    );
    let profile = borg_runtime::native::NativeLoopProfile {
        functions: tools
            .into_iter()
            .map(|t| borg_runtime::native::NativeFunction {
                name: t.name,
                description: t.description,
                parameters: t.input_schema,
            })
            .collect(),
        web_search: None,
    };
    for (field, value) in [
        ("function_count", 64),
        ("schema_bytes", 1),
        ("json_nodes", 1),
    ] {
        let mut input = h.original.input.clone();
        let mut values = input.execution_limits().unwrap().native.clone();
        match field {
            "function_count" => values.function_count = value,
            "schema_bytes" => values.schema_bytes = value,
            _ => values.json_nodes = value,
        }
        input.definition.limit_overrides.as_mut().unwrap().native = Some(values);
        input.resolved_limits = Some(h.cfg.limits.resolve_task(&input).unwrap());
        let sent = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let send = sent.clone();
        let host = native_host(&h, &input);
        let observation = borg_runtime::native::execute_loop_step(
            &input,
            &borg_runtime::native::NativeConfig {
                endpoint: "http://127.0.0.1:9/v1/responses".into(),
                api_key_env: "HOME".into(),
                model: SELECTED_MODEL.into(),
            },
            &profile,
            None,
            &host,
            async move {
                send.store(true, Ordering::SeqCst);
                Ok(())
            },
            std::future::pending(),
        )
        .await;
        assert!(observation.result.is_err(), "{field}");
        assert!(!sent.load(Ordering::SeqCst), "{field} sent provider work");
    }
    h.close().await;
}
fn native_host(h: &Harness, input: &TaskInput) -> borg_runtime::native::ValidationHost {
    let root = h.root.join(id("native-validation"));
    std::fs::create_dir(&root).unwrap();
    borg_runtime::native::ValidationHost::new(
        env!("CARGO_BIN_EXE_borg-runtime").into(),
        root,
        Arc::new(input.execution_limits().unwrap().native.clone()),
        Arc::new(
            borg_runtime::native::ValidationRuntime::new(Arc::new(h.cfg.limits.worker.clone()))
                .unwrap(),
        ),
    )
}
#[tokio::test]
async fn old_readers_and_current_reopen_are_exact() {
    let h = Harness::new("versions", "charter", ExecutorKind::Native, true).await;
    assert_eq!(WIRE_REVISION, 24);
    let refused_call = invocation(&h.caller, "collab_task_create", json!({"agent_id":null}));
    assert_eq!(refused(h.invoke(&refused_call).await), "invalid_input");
    assert!(requires_wire24(&refused_call.operation()));
    let cfg = h.cfg.clone();
    let h = h.reopen(cfg).await;
    let db = h.db.clone();
    let cfg = h.cfg.clone();
    h.close().await;
    let connection = rusqlite::Connection::open(&db).unwrap();
    assert_eq!(
        connection
            .query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        25
    );
    connection.pragma_update(None, "user_version", 24).unwrap();
    drop(connection);
    assert!(Controller::open_with_config(&db, cfg.clone()).is_err());
    let connection = rusqlite::Connection::open(&db).unwrap();
    assert_eq!(
        connection
            .query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        24
    );
    connection.pragma_update(None, "user_version", 25).unwrap();
    drop(connection);
    let c = Controller::open_with_config(&db, cfg).unwrap();
    assert_eq!(
        refused(api(&c, refused_call.operation()).await),
        "invalid_input"
    );
    drop(c);
    owner_closed(&db).await;
}
#[tokio::test]
async fn task_create_default_and_selected_heads_are_frozen() {
    let h = Harness::new("defaults", "charter", ExecutorKind::Native, true).await;
    let mut cfg = h.cfg.clone();
    let p = Arc::make_mut(&mut cfg.limits);
    p.revision = "future-defaults-2".into();
    p.task_defaults.agent_id = Some("reviewer".into());
    let h = h.reopen(cfg).await;
    let call = invocation(&h.caller, "collab_task_create", create_args(None));
    let first = h.invoke(&call).await;
    let t = task(first.clone());
    assert_eq!(t.assigned_agent_id, "reviewer");
    assert_eq!(
        t.input.resolved_limits.as_ref().unwrap().global_revision,
        "future-defaults-2"
    );
    let explicit = task(
        h.call("collab_task_create", create_args(Some("triage")))
            .await,
    );
    assert_eq!(explicit.assigned_agent_id, "triage");
    for value in [
        Value::Null,
        json!(""),
        json!("absent"),
        json!("bad\nidentity"),
    ] {
        let mut a = create_args(None);
        a["agent_id"] = value;
        assert!(matches!(
            h.call("collab_task_create", a).await.outcome,
            Outcome::Refused { .. }
        ))
    }
    let mut a = create_args(Some("triage"));
    a["expected_agent_revision"] = json!("wrong");
    assert_eq!(
        refused(h.call("collab_task_create", a).await),
        "configuration_conflict"
    );
    assert_eq!(h.invoke(&call).await.outcome, first.outcome);
    h.close().await;
}
#[tokio::test]
async fn task_create_replay_lost_response_restart_one_task() {
    let h = Harness::new("create-replay", "charter", ExecutorKind::Native, true).await;
    let call = invocation(&h.caller, "collab_task_create", create_args(None));
    let original = h.invoke(&call).await;
    let created = task(original.clone());
    let lookup = api(
        &h.c,
        Operation::GetCollaborationCall {
            caller: h.caller.clone(),
            operation_id: call.operation_id.clone(),
        },
    )
    .await;
    assert_eq!(
        mcp::recovered(&call, lookup).unwrap().reply.outcome,
        original.outcome
    );
    let mut cfg = h.cfg.clone();
    let p = Arc::make_mut(&mut cfg.limits);
    p.revision = "defaults-future-2".into();
    p.task_defaults.agent_id = Some("reviewer".into());
    let h = h.reopen(cfg).await;
    assert_eq!(h.invoke(&call).await.outcome, original.outcome);
    assert_eq!(
        h.tasks()
            .await
            .iter()
            .filter(|t| t.id == created.id)
            .count(),
        1
    );
    let mut changed = call.clone();
    changed.arguments["instruction"] = json!("changed");
    assert_eq!(refused(h.invoke(&changed).await), "operation_conflict");
    let failure = invocation(
        &h.caller,
        "collab_task_create",
        create_args(Some("reviewer")),
    );
    h.fail.store(true, Ordering::SeqCst);
    assert!(!matches!(
        h.invoke(&failure).await.outcome,
        Outcome::Ok { .. }
    ));
    let second = task(h.invoke(&failure).await);
    assert_eq!(task(h.invoke(&failure).await).id, second.id);
    assert_eq!(h.tasks().await.len(), 3);
    h.close().await;
}
#[tokio::test]
async fn task_create_rejects_identity_and_source_bypasses() {
    let h = Harness::new("sources", "charter", ExecutorKind::Native, true).await;
    for field in [
        "definition",
        "resolved_limits",
        "url",
        "checkpoint_remote",
        "source_endpoint",
        "path",
        "credentials",
        "principal",
    ] {
        let mut a = create_args(None);
        a[field] = json!({});
        assert_eq!(
            refused(h.call("collab_task_create", a).await),
            "invalid_input"
        )
    }
    for source in [
        json!({"registration":{"repository_id":"unknown","registration_revision":"1"},"start_commit":h.source.start_commit}),
        json!({"registration":{"repository_id":"source","registration_revision":"stale"},"start_commit":h.source.start_commit}),
        json!({"registration":h.source.registration,"start_commit":"b".repeat(40)}),
    ] {
        let mut a = create_args(Some("triage"));
        a["repository_read"] = source;
        assert!(matches!(
            h.call("collab_task_create", a).await.outcome,
            Outcome::Refused { .. }
        ));
    }
    assert_eq!(h.tasks().await.len(), 1);
    let mut a = create_args(Some("triage"));
    a["repository_read"] = json!(h.source);
    let native = task(h.call("collab_task_create", a).await);
    assert_eq!(
        native.input.repository_read.unwrap().start_commit,
        h.source.start_commit
    );
    let mut a = create_args(Some("engineer"));
    a["repository"] = json!(h.source);
    a["instruction_source"] = json!(h.source);
    a["output_schema"] = Value::Null;
    let coding = task(h.call("collab_task_create", a).await);
    assert!(coding.input.repository.is_some() && coding.input.instruction_source.is_some());
    let mut a = create_args(Some("triage"));
    a["instruction_source"] = json!(h.source);
    assert!(matches!(
        h.call("collab_task_create", a).await.outcome,
        Outcome::Refused { .. }
    ));
    let cfg = h.cfg.clone();
    let h = h.reopen(cfg).await;
    assert_eq!(h.tasks().await.len(), 3);
    h.close().await;
}
#[tokio::test]
async fn task_create_validation_races_and_limits_refuse_atomically() {
    let h = Harness::new("races", "charter", ExecutorKind::Native, true).await;
    let start = h.clock.load(Ordering::SeqCst);
    let (ready, resume) = h.c.qualification_pause_next_finalization();
    let c = h.c.clone();
    let call = invocation(&h.caller, "collab_task_create", create_args(None));
    let op = call.operation();
    let job = tokio::spawn(async move { api(&c, op).await });
    ready.await.unwrap();
    h.clock.store(start + 2000, Ordering::SeqCst);
    resume.send(()).unwrap();
    let created = task(job.await.unwrap());
    assert_eq!(
        created.input.limits.deadline_unix_ms,
        start + h.cfg.limits.task_defaults.deadline_ms
    );
    let count = h.tasks().await.len();
    let (ready, resume) = h.c.qualification_pause_next_finalization();
    let c = h.c.clone();
    let op = invocation(&h.caller, "collab_task_create", create_args(Some("triage"))).operation();
    let job = tokio::spawn(async move { api(&c, op).await });
    ready.await.unwrap();
    ok(api(
        &h.c,
        Operation::UpdateDefinition {
            operation_id: id("race-update"),
            update: DefinitionUpdate {
                agent_id: "triage".into(),
                expected_revision: "1".into(),
                expected_global_revision: h.cfg.limits.revision.clone(),
                expected_policy_revision: h
                    .cfg
                    .limits
                    .definition_updates
                    .as_ref()
                    .unwrap()
                    .revision
                    .clone(),
                changes: [(DefinitionLimitField::NativeContextBytes, 600000)]
                    .into_iter()
                    .collect(),
            },
        },
    )
    .await);
    resume.send(()).unwrap();
    assert_eq!(refused(job.await.unwrap()), "configuration_conflict");
    assert_eq!(h.tasks().await.len(), count);
    for field in [
        "output_bytes",
        "result_bytes",
        "model_requests",
        "deadline_ms",
    ] {
        let mut a = create_args(None);
        a["workload"] = json!({field:0});
        assert!(matches!(
            h.call("collab_task_create", a).await.outcome,
            Outcome::Refused { .. }
        ))
    }
    let mut stale = invocation(&h.caller, "collab_task_create", create_args(None));
    stale.caller.generation += 1;
    assert!(matches!(
        h.invoke(&stale).await.outcome,
        Outcome::Refused { .. }
    ));
    h.clock.store(
        h.original.input.limits.deadline_unix_ms + 1,
        Ordering::SeqCst,
    );
    assert!(matches!(
        h.call("collab_task_create", create_args(None))
            .await
            .outcome,
        Outcome::Refused { .. }
    ));
    h.close().await;
}
#[tokio::test]
async fn task_create_independence_dependencies_and_history() {
    let h = Harness::new("independent", "charter", ExecutorKind::Native, true).await;
    let before = h.current().await;
    let mut args = create_args(Some("reviewer"));
    args["dependencies"] = json!([before.id]);
    let call = invocation(&h.caller, "collab_task_create", args);
    let created = task(h.invoke(&call).await);
    assert_ne!(created.id, before.id);
    assert_ne!(created.session_id, before.session_id);
    assert!(created.parent_task_id.is_none());
    assert_eq!(h.current().await, before);
    let Data::Collaboration(borg_runtime::collaboration::QueryResult::TaskHistory(history)) =
        ok(api(
            &h.c,
            Operation::QueryCollaboration {
                query: borg_runtime::collaboration::Query::TaskHistory {
                    task_id: created.id.clone(),
                    after: None,
                    limit: 16,
                },
            },
        )
        .await)
    else {
        panic!("canonical history")
    };
    let origin: TaskCreationProvenance = serde_json::from_value(
        history
            .items
            .into_iter()
            .find(|e| e.kind == "created_by")
            .unwrap()
            .body,
    )
    .unwrap();
    assert_eq!(origin.operation_id, call.operation_id);
    assert_eq!(origin.logical_call_id, call.logical_call_id);
    for dependencies in [json!(["unknown-task"]), json!([before.id, before.id])] {
        let mut a = create_args(None);
        a["dependencies"] = dependencies;
        assert!(matches!(
            h.call("collab_task_create", a).await.outcome,
            Outcome::Refused { .. }
        ))
    }
    assert_eq!(h.tasks().await.len(), 2);
    let db = h.db.clone();
    h.close().await;
    let conn =
        rusqlite::Connection::open_with_flags(db, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .unwrap();
    assert_eq!(
        conn.query_row(
            "SELECT json_extract(body,'$.depth') FROM tasks WHERE id=?1",
            [&created.id],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        0
    );
}
