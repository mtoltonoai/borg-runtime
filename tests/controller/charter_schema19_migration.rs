//! Exact retained ordinary schema18 executable supplies the old records.
//! Current-schema23 migration never stamps a current store down to an older form.
use super::*;
const OLD18: &str = "3d974fd1a5577af6ed46d17dd6babb8dc7f8071205c3d851ebbfae543d363183";
fn rows18(p: &std::path::Path) -> BTreeMap<String, Vec<Vec<rusqlite::types::Value>>> {
    let db = rusqlite::Connection::open_with_flags(p, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
        .unwrap();
    let mut q = db
        .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name LIMIT 65")
        .unwrap();
    let tables = q
        .query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(tables.len() <= 64);
    let mut all = BTreeMap::new();
    for table in tables {
        assert!(table.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'));
        let mut q = db
            .prepare(&format!("SELECT * FROM \"{table}\" LIMIT 65"))
            .unwrap();
        let columns = q.column_count();
        assert!(columns <= 32);
        drop(q);
        let order = (1..=columns)
            .map(|n| n.to_string())
            .collect::<Vec<_>>()
            .join(",");
        let mut q = db
            .prepare(&format!(
                "SELECT * FROM \"{table}\" ORDER BY {order} LIMIT 65"
            ))
            .unwrap();
        let values = q
            .query_map([], |r| {
                (0..columns)
                    .map(|i| r.get(i))
                    .collect::<Result<Vec<rusqlite::types::Value>, _>>()
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(values.len() <= 64);
        all.insert(table, values);
    }
    all
}
#[tokio::test]
#[ignore = "requires exact preserved schema18 producer BORG_SCHEMA18_BINARY"]
async fn genuine_schema18_migrates_without_rewriting_and_old_reader_refuses_new_marker() {
    let image =
        PathBuf::from(std::env::var_os("BORG_SCHEMA18_BINARY").expect("preserved18 producer"));
    assert!(image.is_absolute());
    assert_file_hash(&image, OLD18);
    let p = path();
    let root = p.parent().unwrap();
    let cfg = config();
    let limits = cfg.limits.clone();
    let metadata_path = root.join("schema18-limits.json");
    std::fs::write(&metadata_path, serde_json::to_vec(&*limits).unwrap()).unwrap();
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = socket.local_addr().unwrap();
    drop(socket);
    let origin = format!("http://{address}");
    let mut old = HistoricalProducer(
        std::process::Command::new(&image)
            .arg("controller")
            .arg("--limits")
            .arg(&metadata_path)
            .arg("--db")
            .arg(&p)
            .arg("--listen")
            .arg(address.to_string())
            .arg("--origin")
            .arg(&origin)
            .stdin(std::process::Stdio::null())
            .stdout(std::fs::File::create(root.join("schema18.stdout")).unwrap())
            .stderr(std::fs::File::create(root.join("schema18.stderr")).unwrap())
            .spawn()
            .unwrap(),
    );
    let client = HistoricalClient::new(&origin, limits);
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            assert!(old.0.try_wait().unwrap().is_none());
            if let Ok(r) = client
                .call_at(
                    Operation::ListTasks {
                        after: None,
                        limit: 1,
                    },
                    16,
                )
                .await
            {
                assert!(matches!(r.outcome, Outcome::Ok { .. }));
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    let op = Operation::SubmitTask {
        operation_id: "schema18-original-submit".into(),
        input: (native("schema18-original-agent")).into(),
    };
    let original = client.call_at(op.clone(), 16).await.unwrap();
    let task = task(original.outcome.clone());
    assert!(original.commit.is_some());
    assert!(task.input.definition.charter.is_none());
    // Populate both retained classes through the genuine old controller. No
    // direct SQL writes or marker rewriting manufacture an old-format fixture.
    let repository_op = Operation::RegisterRepository {
        operation_id: "schema18-original-repository".into(),
        registration: borg_runtime::repository_catalogue::Registration {
            pull_request_target: None,
            repository_id: "schema18-repository".into(),
            registration_revision: "original-revision".into(),
            display_name: "Original retained repository".into(),
            source_endpoint: "https://example.invalid/schema18-source.git".into(),
            checkpoint_endpoint: "https://example.invalid/schema18-checkpoint.git".into(),
            source_store_key: "schema18-source".into(),
            checkpoint_store_key: "schema18-checkpoint".into(),
        },
    };
    let repository_original = client.call_at(repository_op.clone(), 16).await.unwrap();
    assert!(matches!(&repository_original.outcome, Outcome::Ok { data: Data::RepositoryRegistration(_) }));
    assert!(repository_original.commit.is_some());
    let w = worker("schema18-memory-worker", "original-incarnation", 1, 0);
    let registered = client.call_at(Operation::RegisterWorker {
        operation_id: "schema18-memory-register".into(), worker: w.clone(),
    }, 16).await.unwrap();
    assert!(matches!(registered.outcome, Outcome::Ok { .. }));
    let assigned = client.call_at(Operation::AwaitCommands {
        operation_id: "schema18-memory-await".into(), worker_id: w.id.clone(),
        incarnation: w.incarnation.clone(), acknowledged: vec![],
    }, 16).await.unwrap();
    let Outcome::Ok { data: Data::Commands(commands) } = assigned.outcome else {
        panic!("genuine old assignment expected: {assigned:?}");
    };
    let attempt = run(&commands);
    assert_eq!(attempt.task_id, task.id);
    let started = client.call_at(Operation::ObserveWorker { report: Report {
        report_id: "schema18-memory-started".into(), worker_id: w.id,
        incarnation: w.incarnation, task_id: task.id.clone(), attempt_id: attempt.id.clone(),
        generation: attempt.generation, observation: Observation::Started { thread_id: None, turn_id: None },
    } }, 16).await.unwrap();
    assert!(matches!(started.outcome, Outcome::Ok { data: Data::Task(_) }));
    let memory_op = Operation::SetMemory {
        operation_id: "schema18-original-memory".into(), task_id: task.id.clone(),
        attempt_id: attempt.id, generation: attempt.generation,
        key: "retained-key".into(), expected_revision: None,
        value: json!({"message":"exact original memory", "ordinal":7}),
    };
    let memory_original = client.call_at(memory_op.clone(), 16).await.unwrap();
    assert!(matches!(memory_original.outcome, Outcome::Ok { .. }));
    assert!(memory_original.commit.is_some());
    let memory_query = Operation::GetMemory { agent_id: task.assigned_agent_id.clone(), key: "retained-key".into() };
    let memory_read = client.call_at(memory_query.clone(), 16).await.unwrap();
    assert!(matches!(memory_read.outcome, Outcome::Ok { .. }));
    let repository_queries = [None, Some("original-revision".into())].map(|registration_revision| Operation::GetRepository {
        repository_id: "schema18-repository".into(), registration_revision,
    });
    let mut repository_reads = Vec::new();
    for query in &repository_queries {
        let read = client.call_at(query.clone(), 16).await.unwrap();
        assert_eq!(read.outcome, repository_original.outcome);
        repository_reads.push(read);
    }
    let originals = [(op.clone(), original.clone()), (repository_op, repository_original), (memory_op, memory_original)];
    let mut original_lookups = Vec::new();
    for (operation, result) in &originals {
        let lookup = client.call_at(Operation::GetOperation {
            operation_id: operation.mutation_id().unwrap().into(),
        }, 16).await.unwrap();
        assert_eq!(lookup.commit, result.commit);
        assert!(matches!(lookup.outcome, Outcome::Ok { data: Data::Operation(_) }));
        original_lookups.push(lookup);
    }
    old.stop().await;
    controller_owner_released(&p).await;
    assert_eq!(schema_version(&p), 18);
    let before = rows18(&p);
    assert_eq!(before["memories"].len(), 1, "the genuine old memory must be populated");
    assert_eq!(before["repositories"].len(), 1, "the genuine old head must be populated");
    assert_eq!(before["repository_revisions"].len(), 1, "the original immutable revision must be populated");
    let identity = owner_identity(&p);
    for _ in 0..2 {
        let c = Controller::open_with_config(&p, cfg.clone()).unwrap();
        let replay = reply(&c, op.clone()).await;
        assert_eq!(replay.outcome, original.outcome);
        assert_eq!(replay.commit, original.commit);
        assert_eq!(get(&c, &task.id).await.input, task.input);
        assert_eq!(reply(&c, memory_query.clone()).await.outcome, memory_read.outcome);
        for (query, original_read) in repository_queries.iter().zip(&repository_reads) {
            assert_eq!(reply(&c, query.clone()).await.outcome, original_read.outcome);
        }
        for ((operation, result), original_lookup) in originals.iter().zip(&original_lookups) {
            let replay = reply(&c, operation.clone()).await;
            assert_eq!(replay.outcome, result.outcome);
            assert_eq!(replay.commit, result.commit);
            let lookup = reply(&c, Operation::GetOperation {
                operation_id: operation.mutation_id().unwrap().into(),
            }).await;
            assert_eq!(lookup.outcome, original_lookup.outcome);
            assert_eq!(lookup.commit, original_lookup.commit);
        }
        drop(c);
        controller_owner_released(&p).await;
        assert_eq!(schema_version(&p), 24);
        assert_eq!(rows18(&p), before);
        assert_eq!(owner_identity(&p), identity);
    }
    let bytes = database_bytes(&p);
    let mut reader = HistoricalProducer(
        std::process::Command::new(&image)
            .arg("controller")
            .arg("--limits")
            .arg(&metadata_path)
            .arg("--db")
            .arg(&p)
            .arg("--listen")
            .arg(address.to_string())
            .arg("--origin")
            .arg(&origin)
            .stdin(std::process::Stdio::null())
            .stdout(std::fs::File::create(root.join("old-reader.stdout")).unwrap())
            .stderr(std::fs::File::create(root.join("old-reader.stderr")).unwrap())
            .spawn()
            .unwrap(),
    );
    let status = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(status) = reader.0.try_wait().unwrap() {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert!(!status.success());
    let error = std::fs::read_to_string(root.join("old-reader.stderr")).unwrap();
    assert!(
        error.contains("unsupported") && error.contains("schema"),
        "{error}"
    );
    assert_eq!(database_bytes(&p), bytes);
    assert_eq!(schema_version(&p), 24);
    assert_eq!(rows18(&p), before);
    assert_file_hash(&image, OLD18);
}

fn database_bytes(p: &std::path::Path) -> Vec<(String, Vec<u8>)> {
    ["", "-wal", "-shm"]
        .into_iter()
        .filter_map(|suffix| {
            let name = format!("{}{suffix}", p.display());
            std::fs::read(&name).ok().map(|b| (name, b))
        })
        .collect()
}
