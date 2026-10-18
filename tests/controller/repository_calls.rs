//! Actual local Git + canonical controller/MCP transactions. Reported checkpoint
//! publication is controlled fixture input, not an actual worker/provider run.
use super::*;
use borg_runtime::{
    mcp::{self, Caller, Invocation},
    repository_view::{Data as RepoData, Output},
};
use std::{path::Path, process::Command};
pub(super) fn read_repository_registration() -> borg_runtime::repository_catalogue::Registration {
    borg_runtime::repository_catalogue::Registration {
        pull_request_target: None,
        repository_id: "repository-read-fixture".into(),
        registration_revision: "revision-1".into(),
        display_name: "Local repository read fixture".into(),
        source_endpoint: "source".into(),
        checkpoint_endpoint: "source".into(),
        source_store_key: "bare.git".into(),
        checkpoint_store_key: "bare.git".into(),
    }
}
pub(super) struct Repo {
    pub(super) root: PathBuf,
    pub(super) bare: PathBuf,
    pub(super) old: String,
    pub(super) new: String,
}
fn git(p: &Path, args: &[&str]) -> String {
    let out = Command::new("/usr/bin/git")
        .args([
            "-c",
            "user.name=Fixture",
            "-c",
            "user.email=fixture@invalid",
            "-c",
            "core.hooksPath=/dev/null",
        ])
        .args(args)
        .current_dir(p)
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("HOME", "/nonexistent")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .output()
        .unwrap();
    assert!(out.status.success(), "local Git fixture failed");
    String::from_utf8(out.stdout).unwrap().trim().into()
}
impl Repo {
    pub(super) fn new() -> Self {
        let root = path().parent().unwrap().to_owned();
        let work = root.join("work");
        std::fs::create_dir(&work).unwrap();
        git(&work, &["init", "-q"]);
        std::fs::write(work.join("answer.txt"), "before\nsecond\n").unwrap();
        std::fs::write(
            work.join("waiting.txt"),
            format!("{}\n", "x".repeat(128 * 1024)),
        )
        .unwrap();
        git(&work, &["add", "answer.txt", "waiting.txt"]);
        git(&work, &["commit", "-qm", "first"]);
        let old = git(&work, &["rev-parse", "HEAD"]);
        std::fs::write(work.join("answer.txt"), "after\nsecond\n").unwrap();
        git(&work, &["commit", "-qam", "second"]);
        let new = git(&work, &["rev-parse", "HEAD"]);
        let bare = root.join("bare.git");
        git(
            &root,
            &[
                "clone",
                "--bare",
                "--quiet",
                work.to_str().unwrap(),
                bare.to_str().unwrap(),
            ],
        );
        Self {
            root,
            bare,
            old,
            new,
        }
    }
    pub(super) fn config(&self) -> ControllerConfig {
        let mut cfg = config();
        cfg.repository_read.store_root = Some(self.root.clone());
        cfg
    }
    pub(super) fn input(&self, label: &str) -> TaskInput {
        let mut t = native(label);
        t.repository_read = Some(RepositoryReadSource {
            registration: Some(read_repository_registration().binding()),
            repository: String::new(),
            start_commit: self.old.clone(),
        });
        t
    }
}
impl Drop for Repo {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}
async fn admitted(c: &Controller, label: &str, input: TaskInput) -> (Task, Attempt, Caller) {
    register_repository_fixture(c, &read_repository_registration()).await;
    let coding = input.definition.executor == ExecutorKind::Coding;
    let t = submit(c, &format!("{label}-submit"), input).await;
    let w = worker(
        &format!("{label}-worker"),
        "inc",
        usize::from(!coding),
        usize::from(coding),
    );
    register(c, &w).await;
    let a = run(&commands(c, &w, vec![]).await);
    task(
        report(
            c,
            &a,
            &format!("{label}-started"),
            Observation::Started {
                thread_id: None,
                turn_id: None,
            },
        )
        .await,
    );
    let actor = Caller {
        task_id: t.id.clone(),
        attempt_id: a.id.clone(),
        generation: a.generation,
        worker_id: a.worker_id.clone(),
        incarnation: a.worker_incarnation.clone(),
    };
    (t, a, actor)
}
fn invocation(actor: &Caller, id: &str, tool: &str, args: serde_json::Value) -> Invocation {
    Invocation {
        operation_id: id.into(),
        logical_call_id: format!("logical-{id}"),
        caller: actor.clone(),
        tool: tool.into(),
        arguments: args,
    }
}
fn read_call(actor: &Caller, id: &str, source: &str) -> Invocation {
    invocation(
        actor,
        id,
        "collab_repository_read",
        json!({"source":source,"path":"answer.txt","start_line":1,"max_lines":1}),
    )
}
async fn invoke(c: &Controller, v: &Invocation) -> Reply {
    let r = c
        .handle(Request {
            protocol_revision: WIRE_REVISION,
            request_id: "repository-test".into(),
            command: v.operation(),
        })
        .await;
    r.validate_for(&v.operation()).unwrap();
    r
}
fn output(r: &Reply) -> &Output {
    let Outcome::Ok {
        data: Data::Repository(v),
    } = &r.outcome
    else {
        panic!("unexpected repository reply {r:?}")
    };
    assert!(r.commit.is_some());
    assert!(v.untrusted);
    v
}
async fn checkpoint(c: &Controller, a: &Attempt, id: &str, commit: &str) -> Checkpoint {
    let cp = Checkpoint {
        registration: read_repository_registration().binding(),
        repository: "source".into(),
        commit: commit.into(),
        remote_ref: checkpoint_ref(&a.task_id, &a.id, id),
        operation_id: id.into(),
    };
    assert!(matches!(call(c,Operation::PrepareOperation{operation_id:id.into(),task_id:a.task_id.clone(),attempt_id:a.id.clone(),generation:a.generation,kind:"checkpoint".into(),intended:json!({"registration":cp.registration,"repository":cp.repository,"commit":cp.commit,"remote_ref":cp.remote_ref})}).await,Outcome::Ok{..}));
    assert!(matches!(
        call(
            c,
            Operation::SettleOperation {
                settlement_id: format!("{id}-settled"),
                operation_id: id.into(),
                task_id: a.task_id.clone(),
                attempt_id: a.id.clone(),
                generation: a.generation,
                outcome: json!({"acknowledged":true}),
                checkpoint: Some(cp.clone())
            }
        )
        .await,
        Outcome::Ok { .. }
    ));
    cp
}
// Traceability scope: Real HTTP shared-tool calls execute all five local-Git reads at the accepted commit and reject injected repository/commit/host-path selectors and traversal.
//= spec/impl/v2/implementation.md#repository-reads-through-the-shared-tool-interface
//= type=test
//# The shared agent interface MUST provide read-only repository access within the task's accepted repository scope.
//= spec/impl/v2/implementation.md#repository-reads-through-the-shared-tool-interface
//= type=test
//# A repository read result MUST identify its exact source revision.
#[tokio::test]
async fn repository_calls_actual_http_all_five_exact_sources_and_closed_fields() {
    let f = Repo::new();
    let c = Controller::open_with_config(path(), f.config()).unwrap();
    let (_, _, actor) = admitted(&c, "http-source", f.input("http-source")).await;
    let listener = borg_runtime::server::listen("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let cc = c.clone();
    let bb = base.clone();
    let server = tokio::spawn(async move { borg_runtime::server::serve(cc, listener, bb).await });
    let client = mcp::Client::new(&base, actor.clone(), metadata()).unwrap();
    let tools = client.tools().await.unwrap();
    assert_eq!(tools.len(), 36);
    for name in ["collab_machines_list", "collab_machine_get"] {
        let tool = tools.iter().find(|tool| tool.name == name)
            .expect("accepted telemetry read remains in the closed catalogue");
        assert_eq!(tool.annotations, json!({"readOnlyHint":true,
            "destructiveHint":false,"idempotentHint":false,"openWorldHint":false}));
    }
    assert!(tools.iter().any(|tool| tool.name == "collab_read_continue"));
    assert!(tools.iter().any(|tool| tool.name == "collab_definition_update"));
    assert!(tools.iter().any(|tool| tool.name == "collab_limits_get"));
    assert_eq!(tools, mcp::catalogue(c.limits()));
    // Both HTTP adapters enforce the complete frame using this selected policy.
    assert!(serde_json::to_vec(&tools).unwrap().len() <= c.limits().transport.body_bytes);
    let entries = [
        (
            "tree",
            "collab_repository_tree",
            json!({"source":"start","path":"","after":null,"limit":1}),
        ),
        (
            "file",
            "collab_repository_read",
            json!({"source":"start","path":"answer.txt","start_line":1,"max_lines":1}),
        ),
        (
            "search",
            "collab_repository_search",
            json!({"source":"start","path":"","needle":"before"}),
        ),
        (
            "diff",
            "collab_repository_diff",
            json!({"source":"start","base_source":"start","path":""}),
        ),
        (
            "history",
            "collab_repository_history",
            json!({"source":"start","limit":2}),
        ),
    ];
    for (id, name, args) in entries {
        let r = client
            .call(&invocation(&actor, id, name, args))
            .await
            .unwrap();
        let v = output(&r.reply);
        assert_eq!(v.source.repository, "source");
        assert_eq!(v.source.commit, f.old);
    }
    let r = client
        .call(&read_call(&actor, "file-exact", "start"))
        .await
        .unwrap();
    assert!(
        matches!(&output(&r.reply).data,RepoData::File{text,next_line:Some(2),..}if text=="before\n")
    );
    for (k, v) in [
        ("task_id", json!("another")),
        ("repository", json!("unaccepted")),
        ("commit", json!(f.new)),
        ("host_path", json!(f.bare)),
    ] {
        let mut call = read_call(&actor, &format!("bad-{k}"), "start");
        call.arguments[k] = v;
        assert_eq!(code(invoke(&c, &call).await.outcome), "invalid_input");
    }
    let mut traversal = read_call(&actor, "traversal", "start");
    traversal.arguments["path"] = json!("../answer.txt");
    assert_eq!(
        code(invoke(&c, &traversal).await.outcome),
        "repository_read_invalid"
    );
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
}

// Real local Git and the canonical controller operation path. The executable
// tripwire is synthetic and confined to this test's private repository fixture.
// No claim of protection against a concurrent hostile same-UID maintainer.
// Traceability scope: All five real controller/private-Git reads preserve every repository file byte and mode despite hostile helper configuration; scope is this controlled local repository, not hostile-host isolation.
//= spec/impl/v2/implementation.md#repository-reads-through-the-shared-tool-interface
//= type=test
//# The shared agent interface MUST provide read-only repository access within the task's accepted repository scope.
// Traceability scope: Real controller/Git queries leave a working helper tripwire unexecuted and compare every repository file byte and mode after each query.
//= spec/impl/v2/implementation.md#repository-reads-through-the-shared-tool-interface
//= type=test
//# An agent repository-read operation MUST NOT execute repository-supplied code.
//= spec/impl/v2/implementation.md#repository-reads-through-the-shared-tool-interface
//= type=test
//# An agent repository-read operation MUST NOT modify repository content.
#[tokio::test]
async fn repository_calls_do_not_execute_repo_helpers_or_change_repository_files() {
    use std::collections::BTreeMap;
    use std::os::unix::fs::PermissionsExt;

    fn inventory(root: &Path) -> BTreeMap<PathBuf, (u32, Option<Vec<u8>>)> {
        fn visit(root: &Path, at: &Path, out: &mut BTreeMap<PathBuf, (u32, Option<Vec<u8>>)>) {
            let metadata = std::fs::symlink_metadata(at).unwrap();
            assert!(!metadata.file_type().is_symlink());
            let bytes = if metadata.is_file() {
                Some(std::fs::read(at).unwrap())
            } else {
                assert!(metadata.is_dir());
                for entry in std::fs::read_dir(at).unwrap() {
                    visit(root, &entry.unwrap().path(), out);
                }
                None
            };
            out.insert(
                at.strip_prefix(root).unwrap().to_owned(),
                (metadata.permissions().mode(), bytes),
            );
        }
        let mut out = BTreeMap::new();
        visit(root, root, &mut out);
        out
    }

    let f = Repo::new();
    let marker = f.root.join("repository-helper-executed");
    let helper = f.bare.join("hooks/read-tripwire");
    // Fixture paths have no shell metacharacters. No model or repository input
    // is interpolated into this deliberately hostile local repository helper.
    assert!(
        marker
            .to_str()
            .unwrap()
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || "/-_.".contains(ch))
    );
    std::fs::write(
        &helper,
        format!(
            "#!/bin/sh\nprintf executed >> '{}'\nprintf 'helper-output\\n'\n",
            marker.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&helper, std::fs::Permissions::from_mode(0o700)).unwrap();
    for (key, value) in [
        ("core.hooksPath", helper.parent().unwrap().to_str().unwrap()),
        ("core.fsmonitor", helper.to_str().unwrap()),
        ("credential.helper", helper.to_str().unwrap()),
        ("diff.external", helper.to_str().unwrap()),
        ("diff.tripwire.command", helper.to_str().unwrap()),
        ("diff.tripwire.textconv", helper.to_str().unwrap()),
        ("filter.tripwire.clean", helper.to_str().unwrap()),
        ("filter.tripwire.smudge", helper.to_str().unwrap()),
    ] {
        git(&f.bare, &["config", key, value]);
    }
    for hook in ["post-checkout", "post-merge", "reference-transaction"] {
        std::fs::copy(&helper, f.bare.join("hooks").join(hook)).unwrap();
    }
    std::fs::write(
        f.bare.join("info/attributes"),
        "*.txt diff=tripwire filter=tripwire\n",
    )
    .unwrap();
    // Prove this configuration can execute the tripwire, so its absence below
    // cannot pass merely because a broken/unreachable helper was installed.
    git(&f.bare, &["diff", "--ext-diff", &f.old, &f.new]);
    assert!(std::fs::read(&marker).unwrap().starts_with(b"executed"));
    std::fs::remove_file(&marker).unwrap();
    git(
        &f.bare,
        &["diff", "--no-ext-diff", "--textconv", &f.old, &f.new],
    );
    assert!(std::fs::read(&marker).unwrap().starts_with(b"executed"));
    std::fs::remove_file(&marker).unwrap();

    let c = Controller::open_with_config(path(), f.config()).unwrap();
    let mut input = coding("inert-read");
    input.repository = Some(Repository {
        pull_request_target: None,
        registration: Some(read_repository_registration().binding()),
        url: String::new(),
        start_commit: f.old.clone(),
        checkpoint_remote: String::new(),
    });
    let (_, attempt, actor) = admitted(&c, "inert-read", input).await;
    checkpoint(&c, &attempt, "inert-read-checkpoint", &f.new).await;
    let before = inventory(&f.bare);
    let queries = [
        (
            "tree",
            "collab_repository_tree",
            json!({"source":"checkpoint","path":"","after":null,"limit":8}),
        ),
        (
            "file",
            "collab_repository_read",
            json!({"source":"checkpoint","path":"answer.txt","start_line":1,"max_lines":2}),
        ),
        (
            "search",
            "collab_repository_search",
            json!({"source":"checkpoint","path":"","needle":"after"}),
        ),
        (
            "diff",
            "collab_repository_diff",
            json!({"source":"checkpoint","base_source":"start","path":""}),
        ),
        (
            "history",
            "collab_repository_history",
            json!({"source":"checkpoint","limit":2}),
        ),
    ];
    for (id, tool, arguments) in queries {
        let request = invocation(&actor, &format!("inert-read-{id}"), tool, arguments);
        let reply = invoke(&c, &request).await;
        let result = output(&reply);
        assert_eq!(result.source.commit, f.new);
        assert!(!marker.exists(), "repository helper ran during {id}");
        assert_eq!(
            inventory(&f.bare),
            before,
            "repository content/mode changed during {id}"
        );
    }
}

#[tokio::test]
async fn repository_calls_original_receipt_survives_cancel_restart_and_missing_material() {
    let f = Repo::new();
    let p = path();
    let c = Controller::open_with_config(&p, f.config()).unwrap();
    let (t, _, actor) = admitted(&c, "replay-source", f.input("replay-source")).await;
    let original = read_call(&actor, "read-once", "start");
    let first = invoke(&c, &original).await;
    output(&first);
    let current = get(&c, &t.id).await;
    task(
        call(
            &c,
            Operation::CancelTask {
                operation_id: "cancel-source".into(),
                task_id: t.id.clone(),
                expected_revision: current.revision,
            },
        )
        .await,
    );
    drop(c);
    tokio::time::sleep(Duration::from_millis(40)).await;
    std::fs::remove_dir_all(&f.bare).unwrap();
    let c = Controller::open_with_config(&p, config()).unwrap();
    let replay = invoke(&c, &original).await;
    assert_eq!(first.outcome, replay.outcome);
    assert_eq!(first.commit, replay.commit);
    let mut changed = original.clone();
    changed.arguments["max_lines"] = json!(2);
    assert_eq!(
        code(invoke(&c, &changed).await.outcome),
        "operation_conflict"
    );
    let client_lookup = Operation::GetCollaborationCall {
        caller: actor.clone(),
        operation_id: original.operation_id.clone(),
    };
    let r = c
        .handle(Request {
            protocol_revision: WIRE_REVISION,
            request_id: "lost-ack".into(),
            command: client_lookup,
        })
        .await;
    let found = mcp::recovered(&original, r).unwrap();
    assert_eq!(found.reply.outcome, first.outcome);
    assert!(matches!(
        invoke(&c, &read_call(&actor, "new-after-cancel", "start"))
            .await
            .outcome,
        Outcome::Refused { .. }
    ));
}
#[tokio::test]
async fn repository_calls_no_implicit_association_or_checkpoint_or_mapping() {
    let f = Repo::new();
    let p = path();
    let c = Controller::open_with_config(&p, config()).unwrap();
    let (_, _, actor) = admitted(&c, "mapping-source", f.input("mapping-source")).await;
    let old = read_call(&actor, "missing-mapping", "start");
    let refused = invoke(&c, &old).await;
    assert_eq!(code(refused.outcome.clone()), "repository_read_unavailable");
    assert!(refused.commit.is_some());
    drop(c);
    tokio::time::sleep(Duration::from_millis(40)).await;
    let c = Controller::open_with_config(&p, f.config()).unwrap();
    let again = invoke(&c, &old).await;
    assert_eq!(again.outcome, refused.outcome);
    assert_eq!(again.commit, refused.commit);
    output(&invoke(&c, &read_call(&actor, "new-mapping", "start")).await);
    assert_eq!(
        code(
            invoke(&c, &read_call(&actor, "native-checkpoint", "checkpoint"))
                .await
                .outcome
        ),
        "repository_read_unavailable"
    );
    let (_, _, none) = admitted(&c, "no-source", native("no-source")).await;
    assert_eq!(
        code(
            invoke(&c, &read_call(&none, "no-association", "start"))
                .await
                .outcome
        ),
        "repository_read_unavailable"
    );
}
#[tokio::test]
async fn repository_calls_changed_checkpoint_cannot_retarget_result() {
    let f = Repo::new();
    let c = Controller::open_with_config(path(), f.config()).unwrap();
    let mut input = coding("checkpoint-source");
    input.repository = Some(Repository {
        pull_request_target: None,
        registration: Some(read_repository_registration().binding()),
        url: String::new(),
        start_commit: f.old.clone(),
        checkpoint_remote: String::new(),
    });
    let (_, a, actor) = admitted(&c, "checkpoint-source", input).await;
    checkpoint(&c, &a, "checkpoint-first", &f.old).await;
    let (read, release) = c.qualification_pause_next_finalization();
    let call = read_call(&actor, "checkpoint-read", "checkpoint");
    let cc = c.clone();
    let vv = call.clone();
    let job = tokio::spawn(async move { invoke(&cc, &vv).await });
    read.await.unwrap();
    checkpoint(&c, &a, "checkpoint-second", &f.new).await;
    release.send(()).unwrap();
    assert_eq!(
        code(job.await.unwrap().outcome),
        "repository_source_changed"
    );
    let result = invoke(&c, &read_call(&actor, "checkpoint-current", "checkpoint")).await;
    assert_eq!(output(&result).source.commit, f.new);
    let diff = invocation(
        &actor,
        "checkpoint-diff",
        "collab_repository_diff",
        json!({"source":"checkpoint","base_source":"start","path":"answer.txt"}),
    );
    assert!(
        matches!(&output(&invoke(&c,&diff).await).data,RepoData::Diff{text}if text.contains("+after"))
    );
}
#[tokio::test]
async fn repository_calls_cancel_during_finalization_refuses_and_original_refusal_replays() {
    let f = Repo::new();
    let c = Controller::open_with_config(path(), f.config()).unwrap();
    let (t, _, actor) = admitted(&c, "cancel-source", f.input("cancel-source")).await;
    let (ready, release) = c.qualification_pause_next_finalization();
    let v = read_call(&actor, "cancel-read", "start");
    let cc = c.clone();
    let vv = v.clone();
    let job = tokio::spawn(async move { invoke(&cc, &vv).await });
    ready.await.unwrap();
    let current = tokio::time::timeout(Duration::from_secs(1), get(&c, &t.id))
        .await
        .unwrap();
    task(
        call(
            &c,
            Operation::CancelTask {
                operation_id: "cancel-in-read".into(),
                task_id: t.id,
                expected_revision: current.revision,
            },
        )
        .await,
    );
    release.send(()).unwrap();
    let refused = job.await.unwrap();
    assert_eq!(code(refused.outcome.clone()), "invalid_transition");
    assert!(refused.commit.is_some());
    let replay = invoke(&c, &v).await;
    assert_eq!(refused.outcome, replay.outcome);
    assert_eq!(refused.commit, replay.commit);
}
#[tokio::test]
async fn repository_calls_transport_cancellation_retains_owned_completion_and_receipt() {
    let f = Repo::new();
    let c = Controller::open_with_config(path(), f.config()).unwrap();
    let (_, _, actor) = admitted(&c, "disconnect-source", f.input("disconnect-source")).await;
    let (ready, release) = c.qualification_pause_next_finalization();
    let v = read_call(&actor, "disconnect-read", "start");
    let cc = c.clone();
    let vv = v.clone();
    let job = tokio::spawn(async move { invoke(&cc, &vv).await });
    ready.await.unwrap();
    job.abort();
    assert!(job.await.unwrap_err().is_cancelled());
    release.send(()).unwrap();
    let reply = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let op = Operation::GetCollaborationCall {
                caller: actor.clone(),
                operation_id: v.operation_id.clone(),
            };
            let r = c
                .handle(Request {
                    protocol_revision: WIRE_REVISION,
                    request_id: "recover-disconnected".into(),
                    command: op,
                })
                .await;
            if let Ok(r) = mcp::recovered(&v, r) {
                break r.reply;
            }
            tokio::time::sleep(Duration::from_millis(10)).await
        }
    })
    .await
    .unwrap();
    output(&reply);
    std::fs::remove_dir_all(&f.bare).unwrap();
    assert_eq!(invoke(&c, &v).await.commit, reply.commit);
}
#[tokio::test]
async fn repository_calls_original_deadline_is_rechecked_after_read() {
    let f = Repo::new();
    let clock = Arc::new(AtomicU64::new(100));
    let owned = clock.clone();
    let mut cfg = f.config();
    cfg.clock = Arc::new(move || owned.load(Ordering::SeqCst));
    let c = Controller::open_with_config(path(), cfg).unwrap();
    let mut input = f.input("deadline-source");
    input.limits.deadline_unix_ms = 1000;
    let (_, _, actor) = admitted(&c, "deadline-source", input).await;
    let (ready, release) = c.qualification_pause_next_finalization();
    let v = read_call(&actor, "deadline-read", "start");
    let cc = c.clone();
    let vv = v.clone();
    let job = tokio::spawn(async move { invoke(&cc, &vv).await });
    ready.await.unwrap();
    clock.store(1000, Ordering::SeqCst);
    release.send(()).unwrap();
    let refused = job.await.unwrap();
    assert_eq!(code(refused.outcome.clone()), "invalid_transition");
    assert!(refused.commit.is_some());
    assert_eq!(invoke(&c, &v).await.commit, refused.commit);
}

async fn live_blob(pid: u32) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let state = std::fs::read_to_string(format!("/proc/{pid}/stat")).unwrap();
            let wait = std::fs::read_to_string(format!("/proc/{pid}/wchan")).unwrap();
            if !state.split_once(") ").unwrap().1.starts_with('Z') && wait.contains("pipe_write") {
                assert_eq!(
                    std::fs::read_link(format!("/proc/{pid}/exe")).unwrap(),
                    PathBuf::from("/usr/bin/git")
                );
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("actual Git must be blocked on its bounded stdout pipe");
}
#[tokio::test]
async fn repository_calls_actual_git_wait_releases_sql_and_disconnect_cannot_drop_cleanup() {
    let f = Repo::new();
    let c = Controller::open_with_config(path(), f.config()).unwrap();
    let (t, _, actor) = admitted(&c, "live-wait", f.input("live-wait")).await;
    let (ready, release) =
        borg_runtime::repository_read::qualification_pause_next_blob(f.bare.clone());
    let mut v = read_call(&actor, "live-wait-read", "start");
    v.arguments["path"] = json!("waiting.txt");
    let cc = c.clone();
    let vv = v.clone();
    let job = tokio::spawn(async move { invoke(&cc, &vv).await });
    let pid = ready.await.unwrap();
    live_blob(pid).await;
    // An actual Git process is live while unrelated SQL and cancellation commit.
    let current = tokio::time::timeout(Duration::from_secs(1), get(&c, &t.id))
        .await
        .unwrap();
    job.abort();
    assert!(job.await.unwrap_err().is_cancelled());
    task(
        call(
            &c,
            Operation::CancelTask {
                operation_id: "cancel-live-read".into(),
                task_id: t.id,
                expected_revision: current.revision,
            },
        )
        .await,
    );
    release.send(()).unwrap();
    let recovered = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let r = c
                .handle(Request {
                    protocol_revision: WIRE_REVISION,
                    request_id: "recover-git-read".into(),
                    command: Operation::GetCollaborationCall {
                        caller: actor.clone(),
                        operation_id: v.operation_id.clone(),
                    },
                })
                .await;
            if let Ok(r) = mcp::recovered(&v, r) {
                break r;
            }
            tokio::time::sleep(Duration::from_millis(5)).await
        }
    })
    .await
    .unwrap();
    assert_eq!(code(recovered.reply.outcome.clone()), "invalid_transition");
    assert!(recovered.reply.commit.is_some());
    assert!(
        !PathBuf::from(format!("/proc/{pid}")).exists(),
        "actual Git was reaped before final receipt"
    );
}
#[tokio::test]
async fn repository_calls_actual_git_deadline_kills_and_reaps_before_refusal() {
    let f = Repo::new();
    let clock = Arc::new(AtomicU64::new(100));
    let held = clock.clone();
    let mut cfg = f.config();
    cfg.clock = Arc::new(move || held.load(Ordering::SeqCst));
    let c = Controller::open_with_config(path(), cfg).unwrap();
    let mut input = f.input("live-deadline");
    input.limits.deadline_unix_ms = 1500;
    let (_, _, actor) = admitted(&c, "live-deadline", input).await;
    let (ready, _release) =
        borg_runtime::repository_read::qualification_pause_next_blob(f.bare.clone());
    let mut v = read_call(&actor, "live-deadline-read", "start");
    v.arguments["path"] = json!("waiting.txt");
    let cc = c.clone();
    let job = tokio::spawn(async move { invoke(&cc, &v).await });
    let pid = ready.await.unwrap();
    live_blob(pid).await;
    let refused = tokio::time::timeout(Duration::from_secs(4), job)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(code(refused.outcome), "repository_read_deadline");
    assert!(refused.commit.is_some());
    assert!(
        !PathBuf::from(format!("/proc/{pid}")).exists(),
        "timed out Git must be reaped"
    );
}

fn assert_metadata_refusal(reply: &Reply, expected_code: &str, field: &str) {
    let Outcome::Refused { code, message } = &reply.outcome else {
        panic!("expected metadata refusal, got {reply:?}");
    };
    assert_eq!(code, expected_code);
    assert!(
        message.ends_with(&format!("metadata limit {field} exceeded")),
        "{message}"
    );
}

#[tokio::test]
async fn repository_calls_report_exact_metadata_bound_and_preserve_receipts() {
    let f = Repo::new();
    for case in [
        "metadata_entries",
        "bare_probe_bytes",
        "commit_probe_bytes",
        "git_listing_bytes",
        "size_probe_bytes",
        "file_bytes",
        "output_bytes_envelope",
        "output_bytes_capture",
        "path_bytes",
    ] {
        let mut cfg = f.config();
        let limits = &mut Arc::make_mut(&mut cfg.limits).repository;
        match case {
            "metadata_entries" => limits.metadata_entries = 1,
            "bare_probe_bytes" => limits.bare_probe_bytes = 1,
            "commit_probe_bytes" => limits.commit_probe_bytes = 1,
            "git_listing_bytes" => limits.git_listing_bytes = 1,
            "size_probe_bytes" => limits.size_probe_bytes = 1,
            "file_bytes" => limits.file_bytes = 1,
            // The raw file fits this cap; the serialized source/data envelope does not.
            "output_bytes_envelope" => limits.output_bytes = 64,
            "output_bytes_capture" => limits.output_bytes = 1,
            "path_bytes" => limits.path_bytes = 4,
            _ => unreachable!(),
        }
        let c = Controller::open_with_config(path(), cfg).unwrap();
        let label = format!("metadata-{case}");
        let (_, _, actor) = admitted(&c, &label, f.input(&label)).await;
        let call = match case {
            "path_bytes" => invocation(
                &actor,
                "metadata-read",
                "collab_repository_tree",
                json!({"source":"start","path":"","after":null,"limit":1}),
            ),
            "output_bytes_capture" => invocation(
                &actor,
                "metadata-read",
                "collab_repository_search",
                json!({"source":"start","path":"","needle":"before"}),
            ),
            _ => read_call(&actor, "metadata-read", "start"),
        };
        let (code, field) = match case {
            "path_bytes" => (
                "repository_read_unavailable",
                "repository.path_bytes".into(),
            ),
            "output_bytes_envelope" | "output_bytes_capture" => {
                ("repository_read_bound", "repository.output_bytes".into())
            }
            _ => ("repository_read_bound", format!("repository.{case}")),
        };
        let first = invoke(&c, &call).await;
        assert_metadata_refusal(&first, code, &field);
        assert!(first.commit.is_some());
        let replay = invoke(&c, &call).await;
        assert_eq!(first.outcome, replay.outcome);
        assert_eq!(first.commit, replay.commit);
    }
}

#[tokio::test]
async fn repository_query_metadata_refusal_precedes_ticket_without_control_receipt() {
    let f = Repo::new();
    let mut cfg = f.config();
    let limits = &mut Arc::make_mut(&mut cfg.limits).repository;
    limits.entries_per_page = 2;
    limits.default_entries = 2;
    let c = Controller::open_with_config(path(), cfg).unwrap();
    register_repository_fixture(&c, &read_repository_registration()).await;
    let t = submit(&c, "query-limit-submit", f.input("query-limit")).await;
    let operation = Operation::QueryCollaboration {
        query: borg_runtime::collaboration::Query::Repository {
            task_id: t.id.clone(),
            request: borg_runtime::repository_view::Request::History {
                source: borg_runtime::repository_view::SourceSelector::Start,
                limit: 3,
            },
        },
    };
    let reply = c
        .handle(Request {
            protocol_revision: WIRE_REVISION,
            request_id: "query-limit-read".into(),
            command: operation.clone(),
        })
        .await;
    reply.validate_for(&operation).unwrap();
    assert_metadata_refusal(
        &reply,
        "repository_read_invalid",
        "repository.entries_per_page",
    );
    assert!(reply.commit.is_none());
    assert_eq!(get(&c, &t.id).await, t);
}

#[tokio::test]
async fn repository_metadata_refusal_uses_accepted_policy_after_restart() {
    let f = Repo::new();
    let p = path();
    let mut cfg = f.config();
    Arc::make_mut(&mut cfg.limits).repository.file_bytes = 1;
    let c = Controller::open_with_config(&p, cfg).unwrap();
    let (t, _, actor) = admitted(&c, "accepted-file-bound", f.input("accepted-file-bound")).await;
    let original = read_call(&actor, "accepted-file-read", "start");
    let first = invoke(&c, &original).await;
    assert_metadata_refusal(&first, "repository_read_bound", "repository.file_bytes");
    assert!(first.commit.is_some());
    drop(c);
    controller_owner_released(&p).await;
    // The new service policy permits the file; the task retains its accepted limit.
    let c = Controller::open_with_config(&p, f.config()).unwrap();
    let replay = invoke(&c, &original).await;
    assert_eq!(first.outcome, replay.outcome);
    assert_eq!(first.commit, replay.commit);
    let fresh = invoke(&c, &read_call(&actor, "accepted-file-next", "start")).await;
    assert_metadata_refusal(&fresh, "repository_read_bound", "repository.file_bytes");
    assert!(fresh.commit.is_some());
    let current = get(&c, &t.id).await;
    task(
        call(
            &c,
            Operation::CancelTask {
                operation_id: "accepted-file-cancel".into(),
                task_id: t.id,
                expected_revision: current.revision,
            },
        )
        .await,
    );
    let after_cancel = invoke(&c, &original).await;
    assert_eq!(first.outcome, after_cancel.outcome);
    assert_eq!(first.commit, after_cancel.commit);
    assert!(matches!(
        invoke(
            &c,
            &read_call(&actor, "accepted-file-after-cancel", "start")
        )
        .await
        .outcome,
        Outcome::Refused { .. }
    ));
}

mod ui_repository {
    use super::*;
    include!("ui_repository.rs");
}

#[path = "read_pagination.rs"]
mod read_pagination;
