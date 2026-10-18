//! Clean-state canonical agent/session qualification through real controller SQL.
//! Worker reports are controlled inputs; no provider, Codex or host qualification.
use borg_runtime::{
    collaboration::{Query, QueryResult},
    controller::{Controller, ControllerConfig},
    mcp::{Caller, Invocation},
    protocol::*,
};
use serde_json::json;
use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
static NEXT: AtomicU64 = AtomicU64::new(1);
fn dir() -> PathBuf {
    let p = std::env::temp_dir().join(format!(
        "borg-agent-sessions-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir(&p).unwrap();
    p.join("task.db")
}
fn cfg() -> ControllerConfig {
    let mut policy = borg_runtime::limits::RuntimeLimits::seed().unwrap();
    policy.controller.command_wait_ms = 10;
    ControllerConfig {
        limits: Arc::new(policy),
        validation_executable: env!("CARGO_BIN_EXE_borg-runtime").into(),
        ..Default::default()
    }
}
fn input(agent: &str) -> TaskInput {
    TaskInput { instruction_source: None,
        pull_request: None,
        resolved_limits: None,
        repository_read: None,
        instruction: "controlled session work".into(),
        definition: Definition {
            charter: None,
            limit_overrides: None,
            id: agent.into(),
            revision: "v1".into(),
            executor: ExecutorKind::Native,
            inference: Some(InferenceSettings::selected()),
            execution_profile: Some(ExecutionProfile::CollaborationV1 { web_search: None }),
        },
        repository: None,
        acceptance_commands: vec![],
        output_schema: Some(
            json!({"type":"object","properties":{"answer":{"type":"integer"}},"required":["answer"],"additionalProperties":false}),
        ),
        limits: Limits {
            deadline_unix_ms: now() + 600_000,
            model_requests: 20,
            output_bytes: 4096,
            result_bytes: borg_runtime::limits::RuntimeLimits::seed()
                .unwrap()
                .task_defaults
                .result_bytes,
            max_delegation_depth: 4,
        },
        dependencies: vec![],
        native_workflow: None,
    }
}
fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
async fn x(c: &Controller, command: Operation) -> Reply {
    let r = c
        .handle(Request {
            protocol_revision: WIRE_REVISION,
            request_id: format!("request-{}", NEXT.fetch_add(1, Ordering::SeqCst)),
            command: command.clone(),
        })
        .await;
    r.validate_for(&command).unwrap();
    r
}
fn task(r: Reply) -> Task {
    match r.outcome {
        Outcome::Ok {
            data: Data::Task(t),
        } => *t,
        o => panic!("expected task {o:?}"),
    }
}
fn refused(r: Reply, code: &str) {
    assert!(
        matches!(r.outcome,Outcome::Refused{code:ref c,..} if c==code),
        "{r:?}"
    );
}
async fn submit(c: &Controller, id: &str, t: TaskInput) -> Task {
    task(
        x(
            c,
            Operation::SubmitTask {
                operation_id: id.into(),
                input: t,
            },
        )
        .await,
    )
}
async fn get(c: &Controller, id: &str) -> Task {
    task(x(c, Operation::GetTask { task_id: id.into() }).await)
}
async fn session(c: &Controller, id: &str) -> Session {
    match x(
        c,
        Operation::QueryCollaboration {
            query: Query::Session {
                session_id: id.into(),
            },
        },
    )
    .await
    .outcome
    {
        Outcome::Ok {
            data: Data::Collaboration(QueryResult::Session(s)),
        } => s,
        o => panic!("{o:?}"),
    }
}
async fn sessions(
    c: &Controller,
    agent: Option<&str>,
    after: Option<String>,
    limit: usize,
) -> borg_runtime::collaboration::Page<Session> {
    match x(
        c,
        Operation::QueryCollaboration {
            query: Query::Sessions {
                agent_id: agent.map(Into::into),
                after,
                limit,
            },
        },
    )
    .await
    .outcome
    {
        Outcome::Ok {
            data: Data::Collaboration(QueryResult::Sessions(s)),
        } => s,
        o => panic!("{o:?}"),
    }
}
fn worker(n: usize, c: usize) -> Worker {
    Worker {
        id: "worker".into(),
        incarnation: "incarnation-1".into(),
        native_capacity: n,
        coding_capacity: c,
    }
}
async fn register(c: &Controller, w: &Worker) {
    assert!(matches!(
        x(
            c,
            Operation::RegisterWorker {
                operation_id: format!("register-{}-{}", w.id, w.incarnation),
                worker: w.clone()
            }
        )
        .await
        .outcome,
        Outcome::Ok {
            data: Data::Worker(_)
        }
    ));
}
async fn commands(c: &Controller, w: &Worker, ack: Vec<String>) -> Vec<WorkerCommand> {
    match x(
        c,
        Operation::AwaitCommands {
            operation_id: format!("await-{}", NEXT.fetch_add(1, Ordering::SeqCst)),
            worker_id: w.id.clone(),
            incarnation: w.incarnation.clone(),
            acknowledged: ack,
        },
    )
    .await
    .outcome
    {
        Outcome::Ok {
            data: Data::Commands(v),
        } => v,
        Outcome::Pending { .. } => vec![],
        o => panic!("{o:?}"),
    }
}
fn launch(v: &[WorkerCommand], id: &str) -> (String, Session, Attempt, TaskInput) {
    v.iter()
        .find_map(|c| match c {
            WorkerCommand::RunAttempt {
                command_id,
                session,
                attempt,
                task,
                ..
            } if attempt.task_id == id => Some((
                command_id.clone(),
                session.clone(),
                attempt.clone(),
                task.clone(),
            )),
            _ => None,
        })
        .unwrap_or_else(|| panic!("no launch for {id}: {v:?}"))
}
async fn report(c: &Controller, a: &Attempt, id: &str, observation: Observation) -> Reply {
    x(
        c,
        Operation::ObserveWorker {
            report: Report {
                report_id: id.into(),
                task_id: a.task_id.clone(),
                attempt_id: a.id.clone(),
                generation: a.generation,
                worker_id: a.worker_id.clone(),
                incarnation: a.worker_incarnation.clone(),
                observation,
            },
        },
    )
    .await
}
async fn start(c: &Controller, a: &Attempt, label: &str) {
    task(
        report(
            c,
            a,
            &format!("{label}-started"),
            Observation::Started {
                thread_id: None,
                turn_id: None,
            },
        )
        .await,
    );
}
fn actor(a: &Attempt) -> Caller {
    Caller {
        task_id: a.task_id.clone(),
        attempt_id: a.id.clone(),
        generation: a.generation,
        worker_id: a.worker_id.clone(),
        incarnation: a.worker_incarnation.clone(),
    }
}
fn invoke(a: &Attempt, id: &str, tool: &str, args: serde_json::Value) -> Operation {
    Operation::CallCollaboration {
        call: Invocation {
            operation_id: id.into(),
            logical_call_id: format!("logical-{id}"),
            caller: actor(a),
            tool: tool.into(),
            arguments: args,
        },
    }
}
async fn failure(c: &Controller, a: &Attempt, label: &str) -> Task {
    task(
        report(
            c,
            a,
            &format!("{label}-executor"),
            Observation::ExecutorOutcome {
                success: false,
                summary: "controlled failed executor".into(),
            },
        )
        .await,
    );
    task(
        report(
            c,
            a,
            &format!("{label}-stopped"),
            Observation::ProcessTreeStopped,
        )
        .await,
    );
    task(
        report(
            c,
            a,
            &format!("{label}-candidate"),
            Observation::ResultCandidate {
                summary: "controlled failure".into(),
                checkpoint: None,
                checks_passed: false,
                validation: vec![],
                output: None,
            },
        )
        .await,
    )
}
async fn closed(p: &Path) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(p.with_extension("db.controller-lock"))
                .unwrap();
            if f.try_lock().is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await
        }
    })
    .await
    .unwrap();
}

// Traceability scope: Two dispatched sessions of one definition have distinct session/attempt identities and validate their exact agent/task bindings.
//= spec/impl/v2/implementation.md#definitions-select-execution
//= type=test
//# Every instantiation of an agent MUST have a distinct session identity.
//= spec/impl/v2/implementation.md#definitions-select-execution
//= type=test
//# Every session MUST carry the agent identity it runs as.
#[tokio::test]
// Traceability scope: Two same-agent sessions consume the registered capacity while a third is explicitly blocked by executor_capacity; this is not an operating-system quota test.
//= spec/impl/v2/implementation.md#dependencies-and-admission-are-bounded
//= type=test
//# Admission MUST enforce configured queue and resource bounds.
async fn sessions_concurrent_same_agent_share_identity_not_execution_or_capacity() {
    let c = Controller::open_with_config(dir(), cfg()).unwrap();
    let a = submit(&c, "submit-a", input("shared-agent")).await;
    let b = submit(&c, "submit-b", input("shared-agent")).await;
    let d = submit(&c, "submit-d", input("shared-agent")).await;
    assert_ne!(a.id, a.session_id);
    assert_ne!(a.session_id, b.session_id);
    assert_eq!(a.assigned_agent_id, "shared-agent");
    assert_eq!(a.input.definition, b.input.definition);
    let w = worker(2, 0);
    register(&c, &w).await;
    let v = commands(&c, &w, vec![]).await;
    let (_, sa, aa, ia) = launch(&v, &a.id);
    let (_, sb, ab, ib) = launch(&v, &b.id);
    sa.validate_for(&aa, &ia).unwrap();
    sb.validate_for(&ab, &ib).unwrap();
    assert_ne!(aa.id, ab.id);
    assert_ne!(sa.id, sb.id);
    assert_eq!(
        get(&c, &d.id).await.blocked_reason.as_deref(),
        Some("executor_capacity")
    );
    let first = sessions(&c, Some("shared-agent"), None, 1).await;
    assert_eq!(first.items.len(), 1);
    let next = sessions(&c, Some("shared-agent"), first.next_cursor.clone(), 2).await;
    assert_eq!(next.items.len(), 2);
    assert!(next.next_cursor.is_none());
    refused(
        x(
            &c,
            Operation::QueryCollaboration {
                query: Query::Sessions {
                    agent_id: Some("other".into()),
                    after: first.next_cursor,
                    limit: 1,
                },
            },
        )
        .await,
        "invalid_input",
    );
    let agents = x(
        &c,
        Operation::ListAgents {
            after: None,
            limit: 10,
        },
    )
    .await;
    assert!(
        matches!(agents.outcome,Outcome::Ok{data:Data::Agents(ref a)} if a.len()==1&&a[0].id=="shared-agent")
    );
}
// Traceability scope: Two sessions share agent-keyed memory with CAS/fencing; one cancellation and controller restart preserve the canonical later revision.
//= spec/impl/v2/implementation.md#definitions-select-execution
//= type=test
//# A memory MUST be keyed by agent identity rather than by session.
//= spec/impl/v2/implementation.md#definitions-select-execution
//= type=test
//# An agent's memories MUST persist across every session of that agent identity.
#[tokio::test]
async fn sessions_shared_memory_cas_original_refusal_and_stale_caller() {
    let p = dir();
    let c = Controller::open_with_config(&p, cfg()).unwrap();
    let a = submit(&c, "mem-a", input("memory-agent")).await;
    let b = submit(&c, "mem-b", input("memory-agent")).await;
    let w = worker(2, 0);
    register(&c, &w).await;
    let v = commands(&c, &w, vec![]).await;
    let (_, _, aa, _) = launch(&v, &a.id);
    let (_, _, ab, _) = launch(&v, &b.id);
    start(&c, &aa, "mem-a").await;
    start(&c, &ab, "mem-b").await;
    let write = |a: &Attempt, id: &str, expected: Option<u64>, value| {
        invoke(
            a,
            id,
            "collab_memory_set",
            json!({"key":"shared","expected_revision":expected,"value":value}),
        )
    };
    let r = x(&c, write(&aa, "memory-first", None, 1)).await;
    assert!(
        matches!(r.outcome,Outcome::Ok{data:Data::Memory(Some(ref m))} if m.agent_id=="memory-agent"&&m.revision==1)
    );
    let read = x(
        &c,
        invoke(
            &ab,
            "memory-read",
            "collab_memory_get",
            json!({"key":"shared"}),
        ),
    )
    .await;
    assert_eq!(read.outcome, r.outcome);
    let stale = write(&ab, "memory-stale", None, 2);
    let refused_original = x(&c, stale.clone()).await;
    refused(refused_original.clone(), "stale_revision");
    assert!(refused_original.commit.is_some());
    assert!(
        matches!(x(&c,write(&ab,"memory-second",Some(1),2)).await.outcome,Outcome::Ok{data:Data::Memory(Some(ref m))} if m.revision==2)
    );
    let replay = x(&c, stale).await;
    assert_eq!(replay.outcome, refused_original.outcome);
    assert_eq!(replay.commit, refused_original.commit);
    let mut forged = ab.clone();
    forged.task_id = a.id.clone();
    refused(
        x(&c, write(&forged, "forged-session", Some(2), 3)).await,
        "stale_generation",
    );
    let now = get(&c, &a.id).await;
    task(
        x(
            &c,
            Operation::CancelTask {
                operation_id: "cancel-memory-a".into(),
                task_id: a.id.clone(),
                expected_revision: now.revision,
            },
        )
        .await,
    );
    refused(
        x(&c, write(&aa, "cancelled-session", Some(2), 3)).await,
        "invalid_transition",
    );
    assert!(matches!(
        x(&c, write(&ab, "independent-session", Some(2), 4))
            .await
            .outcome,
        Outcome::Ok { .. }
    ));
    drop(c);
    closed(&p).await;
    let c = Controller::open_with_config(&p, cfg()).unwrap();
    assert_eq!(
        session(&c, &a.session_id).await.agent_id,
        session(&c, &b.session_id).await.agent_id
    );
    assert!(
        matches!(x(&c,Operation::GetMemory{agent_id:"memory-agent".into(),key:"shared".into()}).await.outcome,Outcome::Ok{data:Data::Memory(Some(ref m))} if m.value==json!(4)&&m.revision==3)
    );
}
// Traceability scope: Retry and fresh controller restart preserve the session, change the attempt fence, and reject stale progress.
//= spec/impl/v2/implementation.md#definitions-select-execution
//= type=test
//# Recovery of a session MUST retain its session identity.
#[tokio::test]
async fn sessions_failed_retry_and_restart_keep_session_and_new_attempt_fence() {
    let p = dir();
    let c = Controller::open_with_config(&p, cfg()).unwrap();
    let t = submit(&c, "retry-submit", input("retry-agent")).await;
    let s = session(&c, &t.session_id).await;
    let w = worker(1, 0);
    register(&c, &w).await;
    let v = commands(&c, &w, vec![]).await;
    let (cmd, _, a, _) = launch(&v, &t.id);
    start(&c, &a, "retry").await;
    let failed = failure(&c, &a, "retry").await;
    assert_eq!(failed.status, TaskStatus::Failed);
    assert!(failed.attempt.as_ref().unwrap().stopped);
    let retried = task(
        x(
            &c,
            Operation::RetryTask {
                operation_id: "retry-op".into(),
                task_id: t.id.clone(),
                expected_revision: failed.revision,
            },
        )
        .await,
    );
    assert_eq!(retried.session_id, s.id);
    let v = commands(&c, &w, vec![cmd]).await;
    let (_, s2, a2, i) = launch(&v, &t.id);
    assert_eq!(s2, s);
    assert_eq!(a2.session_id, a.session_id);
    assert!(a2.generation > a.generation);
    assert_ne!(a.id, a2.id);
    s2.validate_for(&a2, &i).unwrap();
    refused(
        report(
            &c,
            &a,
            "old-progress",
            Observation::Progress {
                text: "stale".into(),
                output: String::new(),
                truncated: false,
            },
        )
        .await,
        "stale_generation",
    );
    drop(c);
    closed(&p).await;
    let c = Controller::open_with_config(&p, cfg()).unwrap();
    let v = commands(&c, &w, vec![]).await;
    let (_, s3, a3, _) = launch(&v, &t.id);
    assert_eq!(s3, s2);
    assert_eq!(a3, a2);
    assert_eq!(sessions(&c, None, None, 100).await.items.len(), 1);
}
// Traceability scope: One uncertain session remains fenced while a distinct session of the same agent receives independent execution state.
//= spec/impl/v2/implementation.md#definitions-select-execution
//= type=test
//# A session MUST retain its own execution state.
#[tokio::test]
async fn sessions_uncertain_work_blocks_only_its_session_not_same_agent_new_work() {
    let c = Controller::open_with_config(dir(), cfg()).unwrap();
    let t = submit(&c, "unknown-submit", input("same-agent")).await;
    let w = worker(2, 0);
    register(&c, &w).await;
    let v = commands(&c, &w, vec![]).await;
    let (_, _, a, _) = launch(&v, &t.id);
    start(&c, &a, "unknown").await;
    task(
        report(
            &c,
            &a,
            "unknown-outcome",
            Observation::ExecutionUncertain {
                message: "controlled unknown outcome".into(),
            },
        )
        .await,
    );
    let failed = task(report(&c, &a, "unknown-stopped", Observation::ProcessTreeStopped).await);
    assert_eq!(failed.status, TaskStatus::Failed);
    assert!(failed.attempt.as_ref().unwrap().uncertain);
    refused(
        x(
            &c,
            Operation::RetryTask {
                operation_id: "unknown-retry".into(),
                task_id: t.id.clone(),
                expected_revision: failed.revision,
            },
        )
        .await,
        "reconciliation_required",
    );
    let independent = submit(&c, "independent-submit", input("same-agent")).await;
    let v = commands(&c, &w, vec![]).await;
    let (_, s, b, _) = launch(&v, &independent.id);
    assert_ne!(s.id, t.session_id);
    assert_ne!(b.id, a.id);
    assert!(get(&c, &t.id).await.attempt.unwrap().uncertain);
}
// Traceability scope: A newer revision and attempted reassignment cannot retarget the original definition/session identity.
//= spec/impl/v2/implementation.md#definitions-select-execution
//= type=test
//# An agent identity MUST be the stable identity of its definition.
#[tokio::test]
async fn sessions_revision_and_assignment_cannot_retarget_existing_session() {
    let c = Controller::open_with_config(dir(), cfg()).unwrap();
    let first = submit(&c, "revision-first", input("agent-one")).await;
    let s = session(&c, &first.session_id).await;
    let mut newer = input("agent-one");
    newer.definition.revision = "v2".into();
    let second = submit(&c, "revision-second", newer.clone()).await;
    assert_eq!(session(&c, &first.session_id).await, s);
    let second_session = session(&c, &second.session_id).await;
    assert_eq!(second_session.definition, newer.definition);
    let selected = x(&c, Operation::GetAgent { agent_id: "agent-one".into() }).await.outcome;
    assert!(matches!(&selected, Outcome::Ok { data: Data::Agent(agent) }
        if agent.definition == first.input.definition));
    let other = submit(&c, "revision-other", input("agent-two")).await;
    refused(
        x(
            &c,
            Operation::AssignTask {
                operation_id: "wrong-assignment".into(),
                task_id: first.id.clone(),
                expected_revision: first.revision,
                agent_id: other.assigned_agent_id,
            },
        )
        .await,
        "definition_conflict",
    );
    assert_eq!(get(&c, &first.id).await, first);
    // Explicit submission of v2 preserves the selected v1 head. Both accepted
    // revisions remain assignable to this agent without retargeting a session.
    let assigned = x(
        &c,
        Operation::AssignTask {
            operation_id: "same-assignment".into(),
            task_id: first.id.clone(),
            expected_revision: first.revision,
            agent_id: "agent-one".into(),
        },
    )
    .await;
    let assigned = task(assigned);
    assert!(assigned.revision > first.revision);
    assert_eq!(assigned.input, first.input);
    assert_eq!(assigned.assigned_agent_id, first.assigned_agent_id);
    assert_eq!(assigned.session_id, first.session_id);
    assert_eq!(session(&c, &first.session_id).await, s);
    assert_eq!(x(&c, Operation::GetAgent { agent_id: "agent-one".into() }).await.outcome, selected);
    refused(
        x(&c, Operation::AssignTask {
            operation_id: "same-assignment-stale".into(),
            task_id: first.id.clone(),
            expected_revision: first.revision,
            agent_id: "agent-one".into(),
        }).await,
        "stale_revision",
    );
    assert_eq!(get(&c, &first.id).await, assigned);
    assert_eq!(session(&c, &first.session_id).await, s);
    let current = task(
        x(
            &c,
            Operation::AssignTask {
                operation_id: "same-current-assignment".into(),
                task_id: second.id.clone(),
                expected_revision: second.revision,
                agent_id: "agent-one".into(),
            },
        )
        .await,
    );
    assert!(current.revision > second.revision);
    assert_eq!(current.session_id, second.session_id);
    assert_eq!(current.input, second.input);
    assert_eq!(current.assigned_agent_id, second.assigned_agent_id);
    assert_eq!(session(&c, &second.session_id).await, second_session);
    // Assignment of v2 is a discriminating check that the v1 selected head did
    // not follow the assigned task's definition revision.
    assert_eq!(x(&c, Operation::GetAgent { agent_id: "agent-one".into() }).await.outcome, selected);
    let mut conflict = input("agent-one");
    conflict.definition.execution_profile = Some(ExecutionProfile::CollaborationV1 {
        web_search: Some(NativeSearchProfile { max_tool_calls: 2 }),
    });
    refused(
        x(
            &c,
            Operation::SubmitTask {
                operation_id: "changed-definition".into(),
                input: conflict,
            },
        )
        .await,
        "definition_conflict",
    );
    assert_eq!(get(&c, &first.id).await, assigned);
    assert_eq!(get(&c, &second.id).await, current);
    assert_eq!(session(&c, &first.session_id).await, s);
    assert_eq!(session(&c, &second.session_id).await, second_session);
    assert_eq!(x(&c, Operation::GetAgent { agent_id: "agent-one".into() }).await.outcome, selected);
}
#[tokio::test]
async fn sessions_submission_commit_failure_rolls_back_allocation_and_exact_replay() {
    let p = dir();
    let probe = Arc::new(AtomicBool::new(false));
    let mut config = cfg();
    config.qualification_fail_next_commit = Some(probe.clone());
    let c = Controller::open_with_config(&p, config).unwrap();
    let op = Operation::SubmitTask {
        operation_id: "atomic-submit".into(),
        input: input("atomic-agent"),
    };
    probe.store(true, Ordering::SeqCst);
    let failed = x(&c, op.clone()).await;
    refused(failed.clone(), "store_unavailable");
    assert!(failed.commit.is_none());
    assert!(!probe.load(Ordering::SeqCst));
    assert!(sessions(&c, None, None, 100).await.items.is_empty());
    let accepted = x(&c, op.clone()).await;
    let t = task(accepted.clone());
    assert!(t.session_id.ends_with("00000000000000000001"));
    let replay = x(&c, op).await;
    assert_eq!(accepted.commit, replay.commit);
    assert_eq!(accepted.outcome, replay.outcome);
    assert_eq!(sessions(&c, None, None, 100).await.items.len(), 1);
    drop(c);
    closed(&p).await;
    let c = Controller::open_with_config(&p, cfg()).unwrap();
    assert_eq!(get(&c, &t.id).await, t);
    assert_eq!(session(&c, &t.session_id).await.task_id, t.id);
}
#[tokio::test]
async fn sessions_http_catalogue_and_closed_query_association() {
    let c = Controller::open_with_config(dir(), cfg()).unwrap();
    let t = submit(&c, "http-session", input("http-agent")).await;
    let w = worker(1, 0);
    register(&c, &w).await;
    let v = commands(&c, &w, vec![]).await;
    let (_, s, a, _) = launch(&v, &t.id);
    start(&c, &a, "http").await;
    let listener = borg_runtime::server::listen("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let cc = c.clone();
    let bb = base.clone();
    let server = tokio::spawn(async move { borg_runtime::server::serve(cc, listener, bb).await });
    let client = borg_runtime::mcp::Client::new(&base, actor(&a), c.limits().clone()).unwrap();
    let tools = client.tools().await.unwrap();
    assert_eq!(tools.len(), 34);
    assert!(tools.iter().any(|tool| tool.name == "collab_read_continue"));
    assert!(tools.iter().any(|tool| tool.name == "collab_definition_update"));
    assert!(tools.iter().any(|tool| tool.name == "collab_limits_get"));
    assert_eq!(tools, borg_runtime::mcp::catalogue(c.limits()));
    // The real client and server check the complete HTTP JSON-RPC frame against
    // transport metadata; the returned catalogue has no separate literal cap.
    assert!(serde_json::to_vec(&tools).unwrap().len() <= c.limits().transport.body_bytes);
    for name in [
        "collab_repository_tree",
        "collab_repository_read",
        "collab_repository_search",
        "collab_repository_diff",
        "collab_repository_history",
        "collab_repositories_list",
        "collab_repository_get",
    ] {
        let tool = tools
            .iter()
            .find(|tool| tool.name == name)
            .expect("repository tool remains in the closed catalogue");
        assert_eq!(tool.annotations["readOnlyHint"], true);
        assert_eq!(tool.annotations["destructiveHint"], false);
        assert_eq!(tool.annotations["openWorldHint"], false);
    }
    for (id, name, args) in [
        ("http-get", "collab_session_get", json!({"session_id":s.id})),
        (
            "http-list",
            "collab_sessions_list",
            json!({"agent_id":"http-agent","cursor":null,"limit":1}),
        ),
    ] {
        let Operation::CallCollaboration { call } = invoke(&a, id, name, args) else {
            unreachable!()
        };
        let r = client.call(&call).await.unwrap();
        assert!(r.reply.commit.is_some());
        assert!(matches!(r.reply.outcome, Outcome::Ok { .. }));
    }
    refused(
        x(
            &c,
            invoke(
                &a,
                "closed-session",
                "collab_session_get",
                json!({"session_id":s.id,"worker_id":"forged"}),
            ),
        )
        .await,
        "invalid_input",
    );
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
}
#[test]
fn sessions_closed_codec_and_launch_binding_reject_alias_and_substitution() {
    let mut t = input("canonical");
    let mut raw = serde_json::to_value(&t).unwrap();
    raw["requested_agent_id"] = json!("alias");
    assert!(serde_json::from_value::<TaskInput>(raw).is_err());
    t.resolved_limits = Some(
        borg_runtime::limits::RuntimeLimits::seed()
            .unwrap()
            .resolve_task(&t)
            .unwrap(),
    );
    let a = Attempt {
        id: "attempt".into(),
        session_id: "session".into(),
        task_id: "task".into(),
        generation: 1,
        worker_id: "worker".into(),
        worker_incarnation: "inc".into(),
        phase: AttemptPhase::Assigned,
        deadline_unix_ms: t.limits.deadline_unix_ms,
        stopped: false,
        uncertain: false,
    };
    let s = Session {
        id: "session".into(),
        agent_id: t.definition.id.clone(),
        definition: t.definition.clone(),
        task_id: "task".into(),
    };
    s.validate_for(&a, &t).unwrap();
    for field in ["id", "agent_id", "task_id", "definition"] {
        let mut changed = s.clone();
        match field {
            "id" => changed.id = "other".into(),
            "agent_id" => changed.agent_id = "other".into(),
            "task_id" => changed.task_id = "other".into(),
            _ => changed.definition.revision = "other".into(),
        };
        assert_eq!(
            changed.validate_for(&a, &t),
            Err("session_binding_mismatch")
        );
    }
    for id in [&s.agent_id, &s.task_id, &a.id] {
        let mut changed = s.clone();
        changed.id = id.clone();
        let mut bound = a.clone();
        bound.session_id = id.clone();
        assert!(changed.validate_for(&bound, &t).is_err());
    }
}
#[tokio::test]
async fn sessions_clean_schema_rejects_previous_versions_and_corrupt_association() {
    for version in [7, 8, 9, 10, 11] {
        let p = dir();
        let c = Controller::open_with_config(&p, cfg()).unwrap();
        submit(&c, "old-schema", input("old-agent")).await;
        drop(c);
        closed(&p).await;
        let db = rusqlite::Connection::open(&p).unwrap();
        db.pragma_update(None, "user_version", version).unwrap();
        drop(db);
        let before = std::fs::read(&p).unwrap();
        let err = Controller::open_with_config(&p, cfg()).err().unwrap();
        assert!(err.contains("clean development reset"));
        assert_eq!(std::fs::read(&p).unwrap(), before);
    }
    let p = dir();
    let c = Controller::open_with_config(&p, cfg()).unwrap();
    let t = submit(&c, "corrupt-session", input("corrupt-agent")).await;
    drop(c);
    closed(&p).await;
    let db = rusqlite::Connection::open(&p).unwrap();
    db.execute(
        "UPDATE sessions SET task_id='not-the-task' WHERE id=?1",
        [&t.session_id],
    )
    .unwrap();
    drop(db);
    let before = std::fs::read(&p).unwrap();
    assert!(
        Controller::open_with_config(&p, cfg())
            .err()
            .unwrap()
            .contains("corrupt")
    );
    assert_eq!(std::fs::read(&p).unwrap(), before);
}

#[tokio::test]
async fn sessions_configured_child_gets_distinct_session_parent_continuation_retains_original() {
    let p = dir();
    let c = Controller::open_with_config(&p, cfg()).unwrap();
    let mut parent = input("family");
    let mut child_definition = parent.definition.clone();
    child_definition.revision = "coding-v2".into();
    child_definition.executor = ExecutorKind::Coding;
    let mut limits = parent.limits.clone();
    limits.model_requests = 4;
    limits.result_bytes = 4096;
    parent.native_workflow = Some(Box::new(NativeWorkflow::CodingThenContinue {
        child: CodingTaskTemplate { instruction_source: None,
            pull_request: None,
            instruction_prefix: "configured".into(),
            definition: child_definition,
            repository: Repository {
                pull_request_target: None,
                registration: Some(borg_runtime::repository_catalogue::Revision {
                    repository_id: "sessions-child".into(),
                    registration_revision: "1".into(),
                }),
                url: String::new(),
                start_commit: "a".repeat(40),
                checkpoint_remote: String::new(),
            },
            acceptance_commands: vec![vec!["/bin/true".into()]],
            dependencies: vec![],
            limits,
        },
    }));
    let registration = borg_runtime::repository_catalogue::Registration {
        pull_request_target: None,
        repository_id: "sessions-child".into(),
        registration_revision: "1".into(),
        display_name: "Session child fixture".into(),
        source_endpoint: "file:///controlled/repo.git".into(),
        checkpoint_endpoint: "file:///controlled/repo.git".into(),
        source_store_key: "repo.git".into(),
        checkpoint_store_key: "repo.git".into(),
    };
    assert!(matches!(
        x(
            &c,
            Operation::RegisterRepository {
                operation_id: "register-sessions-child".into(),
                registration
            }
        )
        .await
        .outcome,
        Outcome::Ok {
            data: Data::RepositoryRegistration(_)
        }
    ));
    let t = submit(&c, "parent-submit", parent.clone()).await;
    let original = session(&c, &t.session_id).await;
    let w = worker(1, 1);
    register(&c, &w).await;
    let v = commands(&c, &w, vec![]).await;
    let (parent_command, _, a, _) = launch(&v, &t.id);
    start(&c, &a, "parent").await;
    let delegated = x(
        &c,
        invoke(
            &a,
            "child-create",
            "collab_task_delegate",
            json!({"instruction":"controlled child"}),
        ),
    )
    .await;
    let child = task(delegated.clone());
    assert_ne!(child.id, t.id);
    assert_ne!(child.session_id, t.session_id);
    assert_eq!(child.assigned_agent_id, t.assigned_agent_id);
    let child_session = session(&c, &child.session_id).await;
    assert_eq!(child_session.definition.executor, ExecutorKind::Coding);
    assert_eq!(child.parent_task_id.as_deref(), Some(t.id.as_str()));
    let replay = x(
        &c,
        invoke(
            &a,
            "child-create",
            "collab_task_delegate",
            json!({"instruction":"controlled child"}),
        ),
    )
    .await;
    assert_eq!(replay.commit, delegated.commit);
    assert_eq!(replay.outcome, delegated.outcome);
    assert!(get(&c, &t.id).await.waiting_child_id.is_some());
    task(report(&c, &a, "parent-quiescent", Observation::ProcessTreeStopped).await);
    let v = commands(&c, &w, vec![parent_command]).await;
    let (child_command, _, ca, _) = launch(&v, &child.id);
    assert!(
        v.iter()
            .all(|v| !matches!(v,WorkerCommand::RunAttempt{attempt,..} if attempt.task_id==t.id))
    );
    start(&c, &ca, "child").await;
    let failed = failure(&c, &ca, "child").await;
    assert_eq!(failed.status, TaskStatus::Failed);
    drop(c);
    closed(&p).await;
    let c = Controller::open_with_config(&p, cfg()).unwrap();
    let v = commands(&c, &w, vec![child_command]).await;
    let (_, resumed, ra, ri) = launch(&v, &t.id);
    assert_eq!(resumed, original);
    assert_eq!(ra.session_id, a.session_id);
    assert_ne!(ra.id, a.id);
    resumed.validate_for(&ra, &ri).unwrap();
    assert_eq!(ri.definition, parent.definition);
    let current = get(&c, &t.id).await;
    assert_eq!(current.completed_child.as_ref().unwrap().task_id, child.id);
    assert!(current.waiting_child_id.is_none());
    assert_eq!(session(&c, &child.session_id).await, child_session);
    assert_eq!(sessions(&c, Some("family"), None, 100).await.items.len(), 2);
}

// Distinct agents, identical key: actual caller-bound memory writes/readbacks,
// then original receipt replay and canonical state after the owner is reopened.
#[tokio::test]
async fn distinct_agents_keep_same_key_memory_and_revisions_separate_after_restart_and_replay() {
    const KEY: &str = "same-key";
    fn memory(reply: &Reply, agent: &str, revision: u64, value: &serde_json::Value) {
        let Outcome::Ok { data: Data::Memory(Some(memory)) } = &reply.outcome else {
            panic!("canonical agent memory expected: {reply:?}")
        };
        assert_eq!(memory.agent_id, agent);
        assert_eq!(memory.key, KEY);
        assert_eq!(memory.revision, revision);
        assert_eq!(&memory.value, value);
    }
    let p = dir();
    let c = Controller::open_with_config(&p, cfg()).unwrap();
    let a = submit(&c, "separate-memory-submit-a", input("separate-memory-agent-a")).await;
    let b = submit(&c, "separate-memory-submit-b", input("separate-memory-agent-b")).await;
    assert_ne!(a.id, b.id);
    assert_ne!(a.assigned_agent_id, b.assigned_agent_id);
    assert_ne!(a.session_id, b.session_id);
    let w = worker(2, 0);
    register(&c, &w).await;
    let issued = commands(&c, &w, vec![]).await;
    let (_, sa, aa, ia) = launch(&issued, &a.id);
    let (_, sb, ab, ib) = launch(&issued, &b.id);
    sa.validate_for(&aa, &ia).unwrap();
    sb.validate_for(&ab, &ib).unwrap();
    assert_eq!(sa.agent_id, a.assigned_agent_id);
    assert_eq!(sb.agent_id, b.assigned_agent_id);
    start(&c, &aa, "separate-memory-a").await;
    start(&c, &ab, "separate-memory-b").await;
    let write = |attempt: &Attempt, id: &str, expected: Option<u64>, value: serde_json::Value| {
        invoke(attempt, id, "collab_memory_set",
            json!({"key":KEY,"expected_revision":expected,"value":value}))
    };
    let read = |attempt: &Attempt, id: &str| invoke(attempt, id, "collab_memory_get", json!({"key":KEY}));
    let a1 = json!({"owner":"A","value":1});
    let b1 = json!({"owner":"B","value":17});
    let a2 = json!({"owner":"A","value":2});
    let first_a = write(&aa, "separate-memory-a-first", None, a1.clone());
    let original_a = x(&c, first_a.clone()).await;
    memory(&original_a, &a.assigned_agent_id, 1, &a1);
    assert_eq!(original_a.commit.as_ref().unwrap().operation_id, "separate-memory-a-first");
    let absent_b = x(&c, read(&ab, "separate-memory-b-absent")).await;
    assert!(matches!(absent_b.outcome, Outcome::Ok { data: Data::Memory(None) }),
        "A's identical key must remain absent to B: {absent_b:?}");
    let first_b = write(&ab, "separate-memory-b-first", None, b1.clone());
    let original_b = x(&c, first_b.clone()).await;
    memory(&original_b, &b.assigned_agent_id, 1, &b1);
    assert_eq!(original_b.commit.as_ref().unwrap().operation_id, "separate-memory-b-first");
    memory(&x(&c, read(&aa, "separate-memory-a-read-one")).await, &a.assigned_agent_id, 1, &a1);
    memory(&x(&c, read(&ab, "separate-memory-b-read-one")).await, &b.assigned_agent_id, 1, &b1);
    let update_a = write(&aa, "separate-memory-a-update", Some(1), a2.clone());
    let original_update = x(&c, update_a.clone()).await;
    memory(&original_update, &a.assigned_agent_id, 2, &a2);
    assert_eq!(original_update.commit.as_ref().unwrap().operation_id, "separate-memory-a-update");
    memory(&x(&c, read(&ab, "separate-memory-b-after-a-update")).await, &b.assigned_agent_id, 1, &b1);

    drop(c);
    closed(&p).await;
    let c = Controller::open_with_config(&p, cfg()).unwrap();
    assert_eq!(session(&c, &a.session_id).await.agent_id, a.assigned_agent_id);
    assert_eq!(session(&c, &b.session_id).await.agent_id, b.assigned_agent_id);
    for (agent, revision, value) in [(&a.assigned_agent_id, 2, &a2), (&b.assigned_agent_id, 1, &b1)] {
        memory(&x(&c, Operation::GetMemory { agent_id: agent.clone(), key: KEY.into() }).await,
            agent, revision, value);
    }
    // Replay includes A's older revision-1 write after revision 2 exists. It
    // must return its original receipt, never roll either agent's state back.
    for (command, original) in [(first_a, original_a), (first_b, original_b), (update_a, original_update)] {
        let replay = x(&c, command).await;
        assert_eq!(replay.outcome, original.outcome);
        assert_eq!(replay.commit, original.commit);
    }
    for (agent, revision, value) in [(&a.assigned_agent_id, 2, &a2), (&b.assigned_agent_id, 1, &b1)] {
        memory(&x(&c, Operation::GetMemory { agent_id: agent.clone(), key: KEY.into() }).await,
            agent, revision, value);
    }
    drop(c);
    closed(&p).await;
}
