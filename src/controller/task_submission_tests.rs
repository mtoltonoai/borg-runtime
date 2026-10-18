//! Controlled format fixtures and real retained receipt readers; no provider calls.
use super::*;
use crate::{
    controller::{ControllerConfig, application_schema, compatibility},
    limits::RuntimeLimits,
};
use rusqlite::{Connection as Sql, params};
use serde_json::json;

fn draft() -> TaskDraft {
    TaskDraft {
        instruction_source: None,
        pull_request: None,
        repository_read: None,
        instruction: "route this task".into(),
        definition: None,
        repository: None,
        acceptance_commands: vec![],
        output_schema: Some(json!({"type":"object"})),
        limits: RuntimeLimits::seed().unwrap().task_defaults.at(1).unwrap(),
        dependencies: vec![],
        native_workflow: None,
    }
}
fn definition(id: &str) -> Definition {
    Definition {
        id: id.into(),
        revision: "r1".into(),
        executor: ExecutorKind::Native,
        inference: Some(InferenceSettings::selected()),
        execution_profile: Some(ExecutionProfile::CollaborationV1 { web_search: None }),
        limit_overrides: None,
        charter: None,
    }
}
fn db(version: i64) -> Sql {
    let sql = Sql::open_in_memory().unwrap();
    application_schema::expected_schema(&sql, version).unwrap();
    sql
}
fn record(sql: &Sql, op: &Operation, outcome: &Outcome) {
    sql.execute(
        "UPDATE counters SET value=1 WHERE kind='controller_revision'",
        [],
    )
    .unwrap();
    sql.execute(
        "INSERT INTO receipts(id,request,outcome,cleanup,controller_revision) VALUES(?1,?2,?3,0,1)",
        params![
            op.mutation_id().unwrap(),
            encode(op).unwrap(),
            encode(outcome).unwrap()
        ],
    )
    .unwrap();
}
fn original(sql: &Sql) -> (String, String) {
    sql.query_row("SELECT request,outcome FROM receipts", [], |r| {
        Ok((r.get(0)?, r.get(1)?))
    })
    .unwrap()
}

#[test]
fn draft_omission_is_distinct_from_null_and_explicit_input_stays_canonical() {
    let original = draft();
    let encoded = serde_json::to_value(&original).unwrap();
    assert!(encoded.get("definition").is_none());
    assert_eq!(
        serde_json::from_value::<TaskDraft>(encoded.clone()).unwrap(),
        original
    );
    let mut null = encoded.clone();
    null["definition"] = json!(null);
    assert!(serde_json::from_value::<TaskDraft>(null).is_err());
    let mut invalid = encoded.clone();
    invalid["definition"] = json!({"id":"partial"});
    assert!(serde_json::from_value::<TaskDraft>(invalid).is_err());
    let mut forged = encoded;
    forged["resolved_limits"] = json!(null);
    assert!(serde_json::from_value::<TaskDraft>(forged).is_err());
    let explicit = original.resolve(definition("explicit"));
    let bytes = encode(&explicit).unwrap();
    assert_eq!(
        encode(&decode::<TaskInput>(&bytes).unwrap()).unwrap(),
        bytes
    );
    let mut selected = original;
    selected.definition = Some(explicit.definition.clone());
    assert_eq!(
        selected.resolve(selected.definition.clone().unwrap()),
        explicit
    );
    assert_eq!(
        encode(
            &decode::<Operation>(
                &encode(&Operation::SubmitTask {
                    operation_id: "explicit".into(),
                    input: explicit
                })
                .unwrap()
            )
            .unwrap()
        )
        .unwrap(),
        encode(&Operation::SubmitTask {
            operation_id: "explicit".into(),
            input: selected.resolve(selected.definition.clone().unwrap())
        })
        .unwrap()
    );
}

#[test]
//= spec/impl/v2/implementation.md#default-task-assignment
//= type=test
//# The default task agent MUST be selected through metadata.
fn absent_routing_metadata_roundtrips_and_configured_identity_is_bounded() {
    let mut metadata = RuntimeLimits::seed().unwrap();
    metadata.task_defaults.agent_id = None;
    let before = encode(&metadata).unwrap();
    let parsed = RuntimeLimits::parse(before.as_bytes()).unwrap();
    assert!(parsed.task_defaults.agent_id.is_none());
    assert_eq!(encode(&parsed).unwrap(), before);
    assert!(
        serde_json::to_value(&parsed).unwrap()["task_defaults"]
            .get("agent_id")
            .is_none()
    );
    for id in [
        String::new(),
        "bad\nidentity".into(),
        "x".repeat(metadata.transport.identity_bytes + 1),
    ] {
        metadata.task_defaults.agent_id = Some(id);
        assert!(
            metadata
                .validate()
                .unwrap_err()
                .contains("task_defaults.agent_id")
        );
    }
    metadata.task_defaults.agent_id = Some("configured-role".into());
    metadata.validate().unwrap();
}

#[test]
//= spec/impl/v2/implementation.md#default-task-assignment
//= type=test
//# An explicitly selected task agent MUST take precedence over the default agent identity.
//= spec/impl/v2/implementation.md#default-task-assignment
//= type=test
//# Default task assignment MUST refuse when no configured active registered identity can be resolved.
fn current_default_requires_registered_active_head_and_explicit_never_falls_back() {
    let sql = db(24);
    let mut limits = RuntimeLimits::seed().unwrap();
    limits.task_defaults.agent_id = None;
    assert!(matches!(
        resolve(&Connection::new(&sql, &limits), &draft()),
        Err(Fault::Refused("default_agent_not_configured", _))
    ));
    limits.task_defaults.agent_id = Some("chosen".into());
    assert!(matches!(
        resolve(&Connection::new(&sql, &limits), &draft()),
        Err(Fault::Refused("default_agent_not_found", _))
    ));
    let first = definition("chosen");
    sql.execute(
        "INSERT INTO agents(id,definition) VALUES(?1,?2)",
        params![first.id, encode(&first).unwrap()],
    )
    .unwrap();
    assert_eq!(
        resolve(&Connection::new(&sql, &limits), &draft())
            .unwrap()
            .definition,
        first
    );
    let mut later = first.clone();
    later.revision = "r2".into();
    sql.execute(
        "UPDATE agents SET definition=?1 WHERE id=?2",
        params![encode(&later).unwrap(), later.id],
    )
    .unwrap();
    assert_eq!(
        resolve(&Connection::new(&sql, &limits), &draft())
            .unwrap()
            .definition,
        later
    );
    sql.execute(
        "INSERT INTO agent_retirements VALUES(?1,?2,'retire')",
        params![later.id, later.revision],
    )
    .unwrap();
    assert!(matches!(
        resolve(&Connection::new(&sql, &limits), &draft()),
        Err(Fault::Refused("agent_retired", _))
    ));
    let mut explicit = draft();
    explicit.definition = Some(definition("different-explicit"));
    assert_eq!(
        resolve(&Connection::new(&sql, &limits), &explicit)
            .unwrap()
            .definition,
        explicit.definition.unwrap()
    );
}

#[test]
fn schema23_refuses_draft_and_claimed_metadata_in_refused_original_without_tasks() {
    let cfg = ControllerConfig::default();
    let mut claimed = draft().resolve(definition("legacy"));
    claimed.resolved_limits = Some(cfg.limits.resolve_task(&claimed).unwrap());
    assert!(
        claimed
            .resolved_limits
            .as_ref()
            .unwrap()
            .global_metadata
            .task_defaults
            .agent_id
            .is_some()
    );
    for operation in [
        Operation::SubmitTaskDraft {
            operation_id: "draft-refused".into(),
            input: draft(),
        },
        Operation::SubmitTask {
            operation_id: "claim-refused".into(),
            input: claimed,
        },
    ] {
        let sql = db(23);
        record(
            &sql,
            &operation,
            &Outcome::Refused {
                code: "invalid_input".into(),
                message: "original refusal".into(),
            },
        );
        let before = original(&sql);
        let tx = Connection::new(&sql, &cfg.limits);
        super::super::metadata::read(&tx, operation.mutation_id().unwrap())
            .unwrap()
            .unwrap();
        assert!(validate_existing(&tx, &cfg, 23).is_err());
        assert_eq!(original(&sql), before);
        assert_eq!(
            sql.query_row("SELECT count(*) FROM tasks", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            0
        );
        assert_eq!(
            sql.query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            23
        );
    }
}

#[test]
fn schema23_to24_lineage_migration_is_atomic_and_preserves_session_columns_and_inputs() {
    // Controlled schema23 SQL, not a database emitted by a historical binary.
    // Opaque non-null delivery bytes below exercise column preservation only;
    // the delivery protocols and historical full-store admission are separate.
    fn shape(sql: &Sql) -> Vec<(String, String, Option<String>)> {
        sql.prepare("SELECT type,name,sql FROM sqlite_master ORDER BY type,name")
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap()
    }
    fn session_rows(sql: &Sql) -> Vec<Vec<rusqlite::types::Value>> {
        sql.prepare("SELECT id,seq,agent,task_id,body,memory_start_seq,memory_cursor_seq,memory_pending,workflow_delivery FROM sessions ORDER BY seq")
            .unwrap()
            .query_map([], |r| (0..9).map(|i| r.get(i)).collect())
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap()
    }
    fn lineage_tables(sql: &Sql) -> i64 {
        sql.query_row("SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN ('session_inputs','task_assignments')", [], |r| r.get(0)).unwrap()
    }
    fn duplicate_task_session(sql: &Sql, session: &Session) -> rusqlite::Result<usize> {
        sql.execute("INSERT INTO sessions(id,seq,agent,task_id,body,memory_start_seq,memory_cursor_seq,memory_pending,workflow_delivery) VALUES(?1,2,?2,?3,?4,17,19,NULL,NULL)",
            params![session.id,session.agent_id,session.task_id,encode(session).unwrap()])
    }

    let mut cfg = ControllerConfig::default();
    let mut legacy_limits = (*cfg.limits).clone();
    legacy_limits.task_defaults.agent_id = None;
    cfg.limits = std::sync::Arc::new(legacy_limits);
    let sql = db(23);
    let requested = draft().resolve(definition("legacy-triage"));
    let mut input = requested.clone();
    input.resolved_limits = Some(cfg.limits.resolve_task(&input).unwrap());
    input.execution_limits().unwrap();
    let session = Session {
        id: "legacy-session".into(),
        agent_id: input.definition.id.clone(),
        definition: input.definition.clone(),
        task_id: "legacy-task".into(),
    };
    let task = Task {
        id: session.task_id.clone(),
        session_id: session.id.clone(),
        input: input.clone(),
        assigned_agent_id: session.agent_id.clone(),
        pending_assignment: None,
        status: TaskStatus::Queued,
        revision: 1,
        attempt: None,
        selected_checkpoint: None,
        result: None,
        blocked_reason: None,
        native_retry_block: None,
        progress: String::new(),
        recent_output: String::new(),
        output_truncated: false,
        parent_task_id: None,
        waiting_child_id: None,
        completed_child: None,
    };
    let stored = StoredTask {
        task: task.clone(),
        generation: 0,
        attempt_counter: 0,
        depth: 0,
        delegated_model_requests: 0,
        executor_success: None,
        unresolved_execution: false,
        cleanup_remaining: 1,
        completion_hold: None,
    };
    let stored_bytes = encode(&stored).unwrap();
    let input_bytes = encode(&input).unwrap();
    sql.execute(
        "INSERT INTO agents(id,definition) VALUES(?1,?2)",
        params![input.definition.id, encode(&input.definition).unwrap()],
    )
    .unwrap();
    sql.execute(
        "INSERT INTO definitions(id,revision,body) VALUES(?1,?2,?3)",
        params![
            input.definition.id,
            input.definition.revision,
            encode(&input.definition).unwrap()
        ],
    )
    .unwrap();
    sql.execute("INSERT INTO sessions(id,seq,agent,task_id,body,memory_start_seq,memory_cursor_seq,memory_pending,workflow_delivery) VALUES(?1,1,?2,?3,?4,17,19,?5,?6)",
        params![session.id,session.agent_id,session.task_id,encode(&session).unwrap(),
            " {\"retained\":\"memory-column\"} "," {\"retained\":\"workflow-column\"} "]).unwrap();
    sql.execute("INSERT INTO tasks(id,seq,session,agent,executor,status,worker,incarnation,active,deadline,cleanup,body) VALUES(?1,1,?2,?3,'native','queued',NULL,NULL,0,?4,1,?5)",
        params![task.id,session.id,session.agent_id,i64::try_from(input.limits.deadline_unix_ms).unwrap(),stored_bytes]).unwrap();
    record(
        &sql,
        &Operation::SubmitTask {
            operation_id: "original-submit".into(),
            input: requested,
        },
        &Outcome::Ok {
            data: Data::Task(Box::new(task.clone())),
        },
    );
    let original_receipt = original(&sql);
    let original_shape = shape(&sql);
    let original_sessions = session_rows(&sql);
    assert_eq!(original_sessions.len(), 1);
    assert_eq!(original_sessions[0].len(), 9);
    let columns = sql
        .prepare("SELECT name FROM pragma_table_info('sessions') ORDER BY cid")
        .unwrap()
        .query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        columns,
        [
            "id",
            "seq",
            "agent",
            "task_id",
            "body",
            "memory_start_seq",
            "memory_cursor_seq",
            "memory_pending",
            "workflow_delivery"
        ]
    );
    assert_eq!(lineage_tables(&sql), 0);
    let successor = Session {
        id: "successor-session".into(),
        ..session.clone()
    };
    assert!(
        duplicate_task_session(&sql, &successor).is_err(),
        "schema23 permits only one session per task"
    );
    validate_existing(&Connection::new(&sql, &cfg.limits), &cfg, 23).unwrap();
    compatibility::validate_shape(&sql, 23).unwrap();
    assert_eq!(
        super::super::sessions::input(&Connection::new(&sql, &cfg.limits), &session).unwrap(),
        input
    );

    // The actual SQLite commit veto must undo the rebuilt sessions table,
    // indexes, both added tables, seeded input and version marker together.
    sql.commit_hook(Some(|| true)).unwrap();
    assert!(application_schema::upgrade_from_validated(&sql, 23).is_err());
    assert!(sql.is_autocommit());
    assert_eq!(
        sql.query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        23
    );
    assert_eq!(shape(&sql), original_shape);
    assert_eq!(session_rows(&sql), original_sessions);
    assert_eq!(lineage_tables(&sql), 0);
    assert_eq!(original(&sql), original_receipt);
    assert_eq!(
        sql.query_row("SELECT body FROM tasks WHERE id=?1", [&task.id], |r| r
            .get::<_, String>(
            0
        ))
        .unwrap(),
        stored_bytes
    );
    compatibility::validate_shape(&sql, 23).unwrap();
    sql.commit_hook(None::<fn() -> bool>).unwrap();
    assert!(
        duplicate_task_session(&sql, &successor).is_err(),
        "rollback retains the original uniqueness constraint"
    );

    application_schema::upgrade_from_validated(&sql, 23).unwrap();
    assert_eq!(
        sql.query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        application_schema::VERSION
    );
    assert_eq!(session_rows(&sql), original_sessions);
    assert_eq!(original(&sql), original_receipt);
    assert_eq!(
        sql.query_row("SELECT body FROM tasks WHERE id=?1", [&task.id], |r| r
            .get::<_, String>(
            0
        ))
        .unwrap(),
        stored_bytes
    );
    assert_eq!(lineage_tables(&sql), 2);
    let seeded: (String, String, String, Option<String>, Option<String>) = sql
        .query_row(
            "SELECT session_id,task_id,body,previous_session_id,operation_id FROM session_inputs",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
        )
        .unwrap();
    assert_eq!(
        seeded,
        (session.id.clone(), task.id.clone(), input_bytes, None, None)
    );
    assert_eq!(
        super::super::sessions::input(&Connection::new(&sql, &cfg.limits), &session).unwrap(),
        input
    );
    assert_eq!(
        sql.query_row("SELECT count(*) FROM task_assignments", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        0
    );
    compatibility::validate_shape(&sql, 24).unwrap();

    // Structural capability only: real acceptance/activation lineage is tested
    // through AssignTask in the controller integration child.
    assert_eq!(duplicate_task_session(&sql, &successor).unwrap(), 1);
    let after = session_rows(&sql);
    assert_eq!(after.len(), 2);
    assert_eq!(after[0], original_sessions[0]);
    assert_eq!(
        sql.query_row(
            "SELECT count(*) FROM sessions WHERE task_id=?1",
            [&task.id],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        2
    );
    assert_eq!(
        sql.query_row("SELECT count(*) FROM tasks", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        1
    );
    assert_eq!(
        sql.query_row("SELECT count(*) FROM session_inputs", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        1
    );
    assert_eq!(original(&sql), original_receipt);
    assert_eq!(application_schema::AGENT_DIRECTORY_VERSION, 23);
    assert_eq!(application_schema::SUPPLIED_TURN_VERSION, 22);
}
