//! Genuine schema13 fixtures: the pinned historical binary creates all state.
//! The current Controller only opens it after that old owner has exited.
use super::*;
const IMAGE13: &str = "c6995d5d0967ec1d3c1ed2cbda806a651cd3207053475d9d354bbef977453309";
fn producer13() -> (PathBuf, Vec<u8>, Arc<borg_runtime::limits::RuntimeLimits>) {
    let image=PathBuf::from(std::env::var_os("BORG_SCHEMA13_BINARY")
        .expect("explicit preserved schema13 producer required"));
    assert!(image.is_absolute());assert_eq!(image.file_name().unwrap(),"borg-runtime");
    let retained=image.parent().unwrap();assert_eq!(retained.file_name().unwrap(),"retained");
    let artifact=retained.parent().unwrap();
    assert_file_hash(&image,IMAGE13);
    assert_file_hash(&artifact.join("handoff.json"),"057bf0fbaa3cb25a5480f4f397f1d71d7bd3fc9b9827fd95e0953b7eae1a728b");
    assert_file_hash(&artifact.join("compile.json"),"3ba27fa52ecd2ffac185960672da09c8e190de8a2a9218a4428ebc6b857c436e");
    assert_file_hash(&artifact.join("source.json"),"4f22b55ff6e1238ca835ca5821626013d97aa3ef23ca4c623088624abbfe21b1");
    let compile:serde_json::Value=serde_json::from_slice(&std::fs::read(artifact.join("compile.json")).unwrap()).unwrap();
    assert_eq!(compile["status"],"PASS_COMPILE_ONLY");assert_eq!(compile["exit"],0);
    assert_eq!(compile["driver_reaped"],true);assert_eq!(compile["owned_group_empty"],true);
    assert_eq!(compile["source_commit"],"148582bfd49714636209eb56cbadc9e180782770");
    assert_eq!(compile["source"].as_object().unwrap().len(),331);
    assert_eq!(compile["artifacts"]["borg-runtime"]["sha256"],IMAGE13);
    assert!(compile["drift"].as_object().unwrap().values().all(|v|v.as_array().unwrap().is_empty()));
    let source=PathBuf::from(compile["source_root"].as_str().unwrap());
    for (relative,hash) in compile["source"].as_object().unwrap() {
        assert_file_hash(&source.join(relative),hash.as_str().unwrap());
    }
    let metadata=std::fs::read(source.join("metadata/limits.json")).unwrap();
    let limits=Arc::new(borg_runtime::limits::RuntimeLimits::parse(&metadata).unwrap());
    assert!(limits.workflow.is_some());assert!(limits.charter.is_none());
    assert!(limits.governance.is_none());assert!(limits.native_retry.is_none());
    (image,metadata,limits)
}
async fn call13(client:&HistoricalClient,op:Operation)->Reply {
    tokio::time::timeout(Duration::from_secs(10),client.call(op)).await.unwrap().unwrap()
}
async fn fixture13(p:&std::path::Path)->HistoricalFixture {
    let (image,metadata,limits)=producer13();assert!(!p.exists());
    let root=p.parent().unwrap();let metadata_path=root.join("historical13-limits.json");
    std::fs::write(&metadata_path,metadata).unwrap();
    let socket=std::net::TcpListener::bind("127.0.0.1:0").unwrap();let address=socket.local_addr().unwrap();drop(socket);
    let origin=format!("http://{address}");
    // The independently retained debug image stays outside the database fixture
    // directory; no image copy defeats the finite retained-file byte oracle.
    let mut old=HistoricalProducer(std::process::Command::new(&image)
        .arg("controller").arg("--limits").arg(&metadata_path).arg("--db").arg(p)
        .arg("--listen").arg(address.to_string()).arg("--origin").arg(&origin)
        .stdin(std::process::Stdio::null())
        .stdout(std::fs::File::create(root.join("historical13.stdout")).unwrap())
        .stderr(std::fs::File::create(root.join("historical13.stderr")).unwrap()).spawn().unwrap());
    let client=HistoricalClient::new(&origin,limits.clone());
    tokio::time::timeout(Duration::from_secs(10),async {
        loop {
            assert!(old.0.try_wait().unwrap().is_none(),"historical13 owner exited during readiness");
            if let Ok(r)=client.call(Operation::ListTasks{after:None,limit:1}).await {
                assert!(matches!(r.outcome,Outcome::Ok{..}));break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }).await.expect("historical13 readiness bound");
    let mut originals=Vec::new();let mut tasks=Vec::new();
    for (i,agent) in ["schema13-source","schema13-waiter"].into_iter().enumerate() {
        let mut input=native(agent);input.limits.result_bytes=limits.task_defaults.result_bytes;
        let op=Operation::SubmitTask{operation_id:format!("schema13-submit-{i}"),input};
        let r=Box::pin(call13(&client,op.clone())).await;assert!(r.commit.is_some());
        tasks.push(task(r.outcome.clone()));originals.push((op,r));
    }
    let op=create_document("schema13-document");let r=Box::pin(call13(&client,op.clone())).await;
    assert_document_created(&r);originals.push((op,r));
    let op=Operation::MutateQuestion{operation_id:"schema13-question".into(),mutation:QuestionMutation::Pose{question:PoseQuestion{
        record:RecordRef::Task{task_id:tasks[0].id.clone()},recipient:PrincipalRef::TrustedHostOperator,
        prompt:"Choose one option. Recommended: keep.".into(),
        options:vec![QuestionOption{id:"keep".into(),label:"Keep".into(),explanation:"Keep this choice".into()},
            QuestionOption{id:"change".into(),label:"Change".into(),explanation:"Use the alternative".into()}],recommended_option_id:"keep".into(),
    }}};
    let r=Box::pin(call13(&client,op.clone())).await;
    let question=match &r.outcome {Outcome::Ok{data:Data::QuestionMutation(v)}=>v.question.question.clone(),other=>panic!("old question: {other:?}")};
    assert!(r.commit.is_some());originals.push((op,r));
    let op=Operation::MutateWork{operation_id:"schema13-wait".into(),mutation:WorkMutation::AddWait{
        task_id:tasks[1].id.clone(),expected_revision:0,resolver:ResolverRef::Question{question_id:question.id.clone()}}};
    let r=Box::pin(call13(&client,op.clone())).await;
    assert!(matches!(r.outcome,Outcome::Ok{data:Data::WorkMutation(_)}));assert!(r.commit.is_some());originals.push((op,r));
    let op=Operation::MutateQuestion{operation_id:"schema13-close-question".into(),mutation:QuestionMutation::Cancel{
        question_id:question.id,expected_revision:question.revision,reason:"Original requirement withdrawn".into()}};
    let r=Box::pin(call13(&client,op.clone())).await;
    assert!(matches!(r.outcome,Outcome::Ok{data:Data::QuestionMutation(_)}));assert!(r.commit.is_some());originals.push((op,r));
    for t in &mut tasks {*t=task(Box::pin(call13(&client,Operation::GetTask{task_id:t.id.clone()})).await.outcome);}
    old.stop().await;controller_owner_released(p).await;
    assert_file_hash(&image,IMAGE13);assert_eq!(schema_version(p),13);
    assert_original13(p);
    let mut cfg=config();cfg.limits=limits;
    HistoricalFixture{config:cfg,tasks,originals}
}
fn assert_original13(p:&std::path::Path) {
    let db=rusqlite::Connection::open_with_flags(p,rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    assert_eq!(db.query_row("SELECT count(*) FROM sqlite_master WHERE name IN ('charter_sessions','governance_rules','native_request_coordination')",[],|r|r.get::<_,i64>(0)).unwrap(),0);
    assert_eq!(db.query_row("SELECT count(*) FROM pragma_table_info('receipts') WHERE name='governance'",[],|r|r.get::<_,i64>(0)).unwrap(),0);
    assert!(db.query_row("SELECT count(*) FROM workflow_wait_closures",[],|r|r.get::<_,i64>(0)).unwrap()>0);
}
fn records13(p:&std::path::Path)->BTreeMap<String,Vec<String>> {let mut r=records(p);r.remove("schema");r}

#[tokio::test]
#[ignore="historical-image explicit required: pinned BORG_SCHEMA13_BINARY producer"]
async fn genuine_schema13_upgrades_once_preserving_workflow_and_original_receipts() {
    let p=path();let f=Box::pin(fixture13(&p)).await;
    let before=records13(&p);let all_original=historical_records(&p);let identity=owner_identity(&p);
    for name in ["tasks","sessions","definitions","history","receipts","documents","versions","questions","question_actions","work_heads","waits","closures","events"] {
        assert!(!before[name].is_empty(),"actual13 producer must populate {name}");
    }
    for _ in 0..2 {
        let c=Controller::open_with_config(&p,f.config.clone()).unwrap();
        assert_eq!(owner_identity(&p),identity);
        for t in &f.tasks {assert_eq!(Box::pin(get(&c,&t.id)).await,*t);}
        for (op,original) in &f.originals {
            let replay=Box::pin(reply(&c,op.clone())).await;
            assert_eq!(replay.outcome,original.outcome);assert_eq!(replay.commit,original.commit);
            let lookup=Box::pin(reply(&c,Operation::GetOperation{operation_id:op.mutation_id().unwrap().into()})).await;
            let Outcome::Ok{data:Data::Operation(record)}=lookup.outcome else{panic!("original receipt required")};
            assert_eq!(serde_json::from_value::<Outcome>(record.outcome.unwrap()).unwrap(),original.outcome);
            assert_eq!(lookup.commit,original.commit);
        }
        drop(c);controller_owner_released(&p).await;
        assert_eq!(schema_version(&p),20);assert_eq!(records13(&p),before);
        assert_eq!(historical_records(&p),all_original);assert_empty_application_upgrade(&p);
    }
}

#[tokio::test]
#[ignore="historical-image explicit required: pinned BORG_SCHEMA13_BINARY producer"]
async fn corrupt_genuine_schema13_refuses_before_migration_and_preserves_all_bytes() {
    let p=path();let f=Box::pin(fixture13(&p)).await;let identity=owner_identity(&p);
    // One index/body disagreement in a real old-owner record. No version stamp,
    // schema downgrade, fabricated owner binding or copied current-schema row.
    {
        let db=rusqlite::Connection::open_with_flags(&p,rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE).unwrap();
        assert_eq!(db.execute("UPDATE workflow_documents SET revision=revision+1",[]).unwrap(),1);
    }
    // Finish the fixture-owned read-only projection before freezing the file set;
    // opening SQLite can itself create empty WAL/shared-memory sidecars.
    let saved=records(&p);let before=retained_bytes(&p);
    for _ in 0..2 {
        let error=Controller::open_with_config(&p,f.config.clone()).err().expect("corrupt old workflow must refuse");
        assert!(error.contains("invalid stored collaboration state"),"wrong old-state refusal: {error}");
        controller_owner_released(&p).await;
        assert_eq!(retained_bytes(&p),before);assert_eq!(schema_version(&p),13);
        assert_eq!(owner_identity(&p),identity);assert_eq!(records(&p),saved);assert_original13(&p);
        assert_eq!(retained_bytes(&p),before);
    }
}
