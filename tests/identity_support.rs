//! Shared actual-controller fixture; all writes use canonical operations.
#![allow(dead_code)]
use borg_runtime::{
    charter::*,
    controller::{Controller, ControllerConfig},
    limits::*,
    mcp::{Caller, Invocation},
    protocol::*,
};
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
static NEXT: AtomicU64 = AtomicU64::new(1);
pub fn id(prefix: &str) -> String {
    format!("{prefix}-{}", NEXT.fetch_add(1, Ordering::Relaxed))
}
pub fn policy() -> RuntimeLimits {
    let mut p = RuntimeLimits::seed().unwrap();
    p.revision = "identity-fixture-1".into();
    p.identity_administration = Some(IdentityAdministrationPolicy {
        version: 1,
        revision: "identity-policy-1".into(),
        enabled: true,
        manager_agent_id: "charter".into(),
        complete_definitions: true,
    });
    p.task_creation = Some(TaskCreationPolicy {
        version: 1,
        revision: "task-create-policy-1".into(),
        enabled: true,
    });
    p.task_defaults.agent_id = Some("triage".into());
    p.task_defaults.deadline_ms = 600_000;
    p.controller.command_wait_ms = 10;
    p.execution.native.context_bytes = 524288;
    p.execution.native.schema_bytes = 262144;
    p.execution.native.json_nodes = 65536;
    p.execution.native.function_count = 71;
    p.charter = Some(CharterLimits {
        record_bytes: 262144,
        source_bytes: 32768,
        files: 8,
        clauses: 64,
        metadata_bytes: 16384,
        references: 64,
        reply_items: 64,
        tool_calls: 16,
        reference_depth: 8,
        records: 4096,
        page_items: 16,
        page_bytes: 262144,
    });
    p.governance = Some(GovernanceLimits {
        record_bytes: 262144,
        reason_items: 16,
        reason_bytes: 4096,
        scan_rows: 2048,
        rule_versions: 32,
        configuration_versions: 32,
        shadow_evidence_items: 16,
        completion_continuations: 3,
        override_max_ms: 60000,
    });
    p.workflow = Some(WorkflowLimits {
        record_bytes: 65536,
        text_bytes: 4096,
        options: 8,
        records: 512,
        revisions: 4096,
        page_items: 16,
        page_bytes: 65536,
        waits_per_task: 8,
        graph_scan_nodes: 512,
        events_per_mutation: 16,
        event_bytes: 16384,
        notice_bytes: 16384,
        pending_delivery_bytes: 131072,
        delivery_scan_rows: 32,
    });
    p.validate().unwrap();
    p
}
pub fn charter(label: &str) -> DefinitionCharter {
    let sentence = format!("The agent MUST preserve {label}.");
    DefinitionCharter::Inline {
        source: CharterSource {
            files: vec![CharterFile {
                file: "role.md".into(),
                text: format!("# Purpose\n\n{sentence}\n"),
            }],
            clauses: vec![CharterClauseRef {
                file: "role.md".into(),
                section: "Purpose".into(),
                sentence,
            }],
        },
    }
}
pub fn definition(
    p: &RuntimeLimits,
    name: &str,
    executor: ExecutorKind,
    admin: bool,
) -> Definition {
    let mut collaboration = p.execution.collaboration.clone();
    collaboration.capabilities = Some(CollaborationCapabilities {
        version: 1,
        task_creation: true,
        identity_administration: admin,
    });
    Definition {
        id: name.into(),
        revision: "1".into(),
        executor,
        inference: Some(InferenceSettings::selected()),
        execution_profile: Some(ExecutionProfile::CollaborationV2 { web_search: None }),
        charter: Some(charter(name)),
        limit_overrides: Some(Box::new(ExecutionLimitOverrides {
            native: None,
            coding: None,
            collaboration: Some(collaboration),
            memory_notice: None,
        })),
    }
}
pub async fn api(c: &Controller, command: Operation) -> Reply {
    let r = Box::pin(c.handle(Request {
        protocol_revision: WIRE_REVISION,
        request_id: id("request"),
        command: command.clone(),
    }))
    .await;
    r.validate_for(&command).unwrap();
    r
}
pub fn ok(r: Reply) -> Data {
    match r.outcome {
        Outcome::Ok { data } => data,
        other => panic!("expected committed success: {other:?}"),
    }
}
pub fn task(r: Reply) -> Task {
    match ok(r) {
        Data::Task(t) => *t,
        other => panic!("expected task: {other:?}"),
    }
}
pub fn refused(r: Reply) -> String {
    match r.outcome {
        Outcome::Refused { code, .. } => code,
        other => panic!("expected refusal: {other:?}"),
    }
}
pub async fn selected(c: &Controller, name: &str) -> AgentSummary {
    match ok(api(
        c,
        Operation::GetAgent {
            agent_id: name.into(),
        },
    )
    .await)
    {
        Data::Agent(a) => a,
        other => panic!("agent {other:?}"),
    }
}
pub fn bound_actor(h: &Harness) -> borg_runtime::collaboration::workflow::RecordedActor {
    use borg_runtime::collaboration::workflow::{ActorProvenance, PrincipalRef, RecordedActor};
    RecordedActor {
        principal: PrincipalRef::Agent {
            agent_id: h.original.assigned_agent_id.clone(),
        },
        provenance: ActorProvenance::BoundAgentSession {
            agent_id: h.original.assigned_agent_id.clone(),
            session_id: h.original.session_id.clone(),
            task_id: h.caller.task_id.clone(),
            attempt_id: h.caller.attempt_id.clone(),
            generation: h.caller.generation,
            worker_id: h.caller.worker_id.clone(),
            incarnation: h.caller.incarnation.clone(),
        },
    }
}
pub async fn creation(h: &Harness, task_id: &str) -> TaskCreationProvenance {
    let Data::Collaboration(borg_runtime::collaboration::QueryResult::TaskHistory(history)) =
        ok(api(
            &h.c,
            Operation::QueryCollaboration {
                query: borg_runtime::collaboration::Query::TaskHistory {
                    task_id: task_id.into(),
                    after: None,
                    limit: 16,
                },
            },
        )
        .await)
    else {
        panic!("canonical creation history")
    };
    let mut origins = history.items.into_iter().filter(|e| e.kind == "created_by");
    let value = serde_json::from_value(origins.next().unwrap().body).unwrap();
    assert!(origins.next().is_none());
    value
}
pub fn invocation(caller: &Caller, tool: &str, args: Value) -> Invocation {
    Invocation {
        operation_id: id("operation"),
        logical_call_id: id("logical"),
        caller: caller.clone(),
        tool: tool.into(),
        arguments: args,
    }
}
pub fn admin_args(p: &RuntimeLimits, fields: Value) -> Value {
    let mut a = fields.as_object().unwrap().clone();
    a.insert("expected_global_revision".into(), json!(p.revision));
    a.insert(
        "expected_policy_revision".into(),
        json!(p.identity_administration.as_ref().unwrap().revision),
    );
    a.insert("expected_governance_revision".into(), json!(p.revision));
    Value::Object(a)
}
pub fn create_args(agent: Option<&str>) -> Value {
    let mut a = json!({"instruction":"An independent task with an actual canonical result","dependencies":[],"acceptance_commands":[],"output_schema":{"type":"integer"}});
    if let Some(agent) = agent {
        a["agent_id"] = json!(agent)
    }
    a
}
pub async fn owner_closed(db: &Path) {
    let lock = PathBuf::from(format!("{}.controller-lock", db.display()));
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&lock)
                .unwrap();
            if f.try_lock().is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("controller and actual validator owner closure");
}
fn git(root: &Path, args: &[&str]) -> String {
    let out = std::process::Command::new("/usr/bin/git")
        .arg("-C")
        .arg(root)
        .args(args)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "git fixture {:?}: {}",
        args,
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap().trim().into()
}
pub struct Harness {
    pub c: Controller,
    pub cfg: ControllerConfig,
    pub root: PathBuf,
    pub db: PathBuf,
    pub clock: Arc<AtomicU64>,
    pub fail: Arc<AtomicBool>,
    pub source: TaskRepositorySelection,
    pub caller: Caller,
    pub original: Task,
}
impl Harness {
    pub async fn new(label: &str, agent: &str, executor: ExecutorKind, admin: bool) -> Self {
        Self::with_policy(label, agent, executor, admin, policy()).await
    }
    pub async fn with_policy(
        label: &str,
        agent: &str,
        executor: ExecutorKind,
        admin: bool,
        p: RuntimeLimits,
    ) -> Self {
        let root = std::env::temp_dir().join(format!(
            "borg-identity-{}-{}-{}",
            label,
            std::process::id(),
            id("root")
        ));
        std::fs::create_dir_all(&root).unwrap();
        let work = root.join("work");
        std::fs::create_dir(&work).unwrap();
        git(&work, &["init", "-q"]);
        std::fs::write(work.join("README.md"), "Explicit test repository scope\n").unwrap();
        git(&work, &["add", "README.md"]);
        git(
            &work,
            &[
                "-c",
                "user.name=Fixture",
                "-c",
                "user.email=fixture@example.invalid",
                "commit",
                "-qm",
                "fixture",
            ],
        );
        let commit = git(&work, &["rev-parse", "HEAD"]);
        git(
            &root,
            &["clone", "-q", "--bare", work.to_str().unwrap(), "source"],
        );
        let clock = Arc::new(AtomicU64::new(borg_runtime::worker::process::now_ms()));
        let cl = clock.clone();
        let fail = Arc::new(AtomicBool::new(false));
        let cfg = ControllerConfig {
            limits: Arc::new(p),
            validation_executable: env!("CARGO_BIN_EXE_borg-runtime").into(),
            clock: Arc::new(move || cl.load(Ordering::SeqCst)),
            repository_read: borg_runtime::repository_read::Config {
                store_root: Some(root.clone()),
            },
            qualification_fail_next_commit: Some(fail.clone()),
            ..Default::default()
        };
        let db = root.join("tasks.sqlite");
        let c = Controller::open_with_config(&db, cfg.clone()).unwrap();
        let registration = borg_runtime::repository_catalogue::Registration {
            repository_id: "source".into(),
            registration_revision: "1".into(),
            display_name: "Known fixture source".into(),
            source_endpoint: root.join("source").display().to_string(),
            checkpoint_endpoint: root.join("source").display().to_string(),
            source_store_key: "source".into(),
            checkpoint_store_key: "source".into(),
            pull_request_target: None,
        };
        let source = TaskRepositorySelection {
            registration: registration.binding(),
            start_commit: commit,
        };
        ok(api(
            &c,
            Operation::RegisterRepository {
                operation_id: id("register-source"),
                registration,
            },
        )
        .await);
        let p = &cfg.limits;
        let mut definitions = vec![
            definition(p, "charter", ExecutorKind::Native, true),
            definition(p, "triage", ExecutorKind::Native, false),
            definition(p, "engineer", ExecutorKind::Coding, false),
            definition(p, "reviewer", ExecutorKind::Native, false),
        ];
        if let Some(d) = definitions.iter_mut().find(|d| d.id == agent) {
            *d = definition(p, agent, executor, admin)
        } else {
            definitions.push(definition(p, agent, executor, admin))
        }
        for d in definitions {
            ok(api(
                &c,
                Operation::RegisterAgent {
                    operation_id: id("register"),
                    definition: d,
                },
            )
            .await);
        }
        let d = selected(&c, agent).await.definition;
        let input = TaskInput {
            definition: d,
            instruction: "Bound caller fixture".into(),
            repository: if executor == ExecutorKind::Coding {
                Some(Repository {
                    registration: Some(source.registration.clone()),
                    start_commit: source.start_commit.clone(),
                    url: String::new(),
                    checkpoint_remote: String::new(),
                    pull_request_target: None,
                })
            } else {
                None
            },
            repository_read: None,
            instruction_source: None,
            pull_request: None,
            resolved_limits: None,
            output_schema: if executor == ExecutorKind::Native {
                Some(json!({"type":"integer"}))
            } else {
                None
            },
            acceptance_commands: vec![],
            dependencies: vec![],
            native_workflow: None,
            limits: p.task_defaults.at(clock.load(Ordering::SeqCst)).unwrap(),
        };
        let original = task(
            api(
                &c,
                Operation::SubmitTask {
                    operation_id: id("submit-caller"),
                    input,
                },
            )
            .await,
        );
        let worker = Worker {
            id: "controlled-worker".into(),
            incarnation: "1".into(),
            native_capacity: 1,
            coding_capacity: 1,
        };
        ok(api(
            &c,
            Operation::RegisterWorker {
                operation_id: id("worker"),
                worker: worker.clone(),
            },
        )
        .await);
        let Data::Commands(commands) = ok(api(
            &c,
            Operation::AwaitCommands {
                operation_id: id("await"),
                worker_id: worker.id.clone(),
                incarnation: worker.incarnation.clone(),
                acknowledged: vec![],
            },
        )
        .await) else {
            panic!("commands")
        };
        let a = commands
            .into_iter()
            .find_map(|c| match c {
                WorkerCommand::RunAttempt { attempt, .. } if attempt.task_id == original.id => {
                    Some(attempt)
                }
                _ => None,
            })
            .expect("actual admitted caller");
        ok(api(
            &c,
            Operation::ObserveWorker {
                report: Report {
                    report_id: id("started"),
                    worker_id: a.worker_id.clone(),
                    incarnation: a.worker_incarnation.clone(),
                    task_id: a.task_id.clone(),
                    attempt_id: a.id.clone(),
                    generation: a.generation,
                    observation: Observation::Started {
                        thread_id: None,
                        turn_id: None,
                    },
                },
            },
        )
        .await);
        let caller = Caller {
            task_id: a.task_id,
            attempt_id: a.id,
            generation: a.generation,
            worker_id: a.worker_id,
            incarnation: a.worker_incarnation,
        };
        Self {
            c,
            cfg,
            root,
            db,
            clock,
            fail,
            source,
            caller,
            original,
        }
    }
    pub async fn invoke(&self, call: &Invocation) -> Reply {
        api(&self.c, call.operation()).await
    }
    pub async fn call(&self, name: &str, args: Value) -> Reply {
        self.invoke(&invocation(&self.caller, name, args)).await
    }
    pub async fn tasks(&self) -> Vec<Task> {
        match ok(api(
            &self.c,
            Operation::ListTasks {
                after: None,
                limit: 100,
            },
        )
        .await)
        {
            Data::Tasks(t) => t,
            _ => panic!("tasks"),
        }
    }
    pub async fn current(&self) -> Task {
        task(
            api(
                &self.c,
                Operation::GetTask {
                    task_id: self.original.id.clone(),
                },
            )
            .await,
        )
    }
    pub async fn register(&self, name: &str) -> Reply {
        self.call(
            "collab_agent_register",
            admin_args(
                &self.cfg.limits,
                json!({"definition":definition(&self.cfg.limits,name,ExecutorKind::Native,false)}),
            ),
        )
        .await
    }
    pub async fn reopen(self, cfg: ControllerConfig) -> Self {
        let Self {
            c,
            root,
            db,
            clock,
            fail,
            source,
            caller,
            original,
            ..
        } = self;
        drop(c);
        owner_closed(&db).await;
        let c = Controller::open_with_config(&db, cfg.clone()).unwrap();
        Self {
            c,
            cfg,
            root,
            db,
            clock,
            fail,
            source,
            caller,
            original,
        }
    }
    pub async fn close(self) {
        let db = self.db.clone();
        drop(self);
        owner_closed(&db).await;
    }
}
