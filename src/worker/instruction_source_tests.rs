//! Bridge identity/recovery discriminators; transport is the existing Fake.
use super::*;

fn source() -> RepositoryReadSource {
    RepositoryReadSource {
        registration: Some(crate::repository_catalogue::Revision {
            repository_id: "spec".into(),
            registration_revision: "r1".into(),
        }),
        repository: "/provisioned/spec.git".into(),
        start_commit: "a".repeat(40),
    }
}
#[tokio::test]
async fn instruction_bridge_replays_exact_source_and_refuses_retarget_before_dispatch() {
    let (_area, mut cfg, fake, cancel) = setup(Mode::LossAfter);
    cfg.instruction_source = Some(source());
    let path = cfg.directory().join("calls.json");
    let args = json!({"source":"instructions","path":"AGENTS.md","start_line":1,"max_lines":4});
    let mut bridge = Bridge::open(cfg.clone(), fake.clone(), vec![], cancel.clone()).unwrap();
    let (first, index) = bridge
        .invoke(
            "collab_repository_read",
            args.clone(),
            &meta("original-read"),
        )
        .await
        .unwrap();
    bridge.delivered(index).unwrap();
    let bytes = std::fs::read(&path).unwrap();
    let original: Journal = decode(&bytes, cfg.limits.journal_bytes).unwrap();
    assert_eq!(original.instruction_source, cfg.instruction_source);
    assert_eq!(original.entries[0].invocation.arguments, args);
    assert_eq!(fake.state.lock().unwrap().calls, 1);
    assert_eq!(fake.state.lock().unwrap().lookups, 1);
    drop(bridge);
    for changed in [
        None,
        Some(RepositoryReadSource {
            start_commit: "b".repeat(40),
            ..source()
        }),
    ] {
        let mut bad = cfg.clone();
        bad.instruction_source = changed;
        assert!(Bridge::open(bad, fake.clone(), vec![], cancel.clone()).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
        assert_eq!(fake.state.lock().unwrap().calls, 1);
    }
    let mut resumed = Bridge::open(cfg.clone(), fake.clone(), vec![], cancel).unwrap();
    let (replay, _) = resumed
        .invoke("collab_repository_read", args, &meta("original-read"))
        .await
        .unwrap();
    assert_eq!(first, replay);
    assert_eq!(fake.state.lock().unwrap().calls, 1);
    drop(resumed);
    assert_eq!(
        outcome_disposition(&cfg).unwrap(),
        OutcomeDisposition::Committed
    );
    let mut tampered = cfg.clone();
    tampered.instruction_source = None;
    process::atomic_json(&cfg.state.join("borg-collaboration-config.json"), &tampered).unwrap();
    assert!(
        outcome_disposition(&cfg)
            .unwrap_err()
            .contains("instruction source")
    );
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
}

#[test]
fn instruction_bridge_absence_and_exact_config_bound_preserve_original_contract() {
    let (_area, mut cfg, _fake, _cancel) = setup(Mode::Good);
    let old = serde_json::to_vec(&cfg).unwrap();
    assert!(
        serde_json::from_slice::<Value>(&old)
            .unwrap()
            .get("instruction_source")
            .is_none()
    );
    assert_eq!(
        serde_json::to_vec(&serde_json::from_slice::<Config>(&old).unwrap()).unwrap(),
        old
    );
    cfg.instruction_source = Some(source());
    cfg.validate().unwrap();
    let encoded = serde_json::to_vec(&cfg).unwrap();
    let path = cfg.state.join("exact-config.json");
    bounded_save(
        &path,
        &cfg,
        encoded.len(),
        "execution.collaboration.config_bytes",
    )
    .unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), encoded);
    let refused = cfg.state.join("too-small-config.json");
    assert!(
        bounded_save(
            &refused,
            &cfg,
            encoded.len() - 1,
            "execution.collaboration.config_bytes"
        )
        .is_err()
    );
    assert!(!refused.exists());
    for invalid in [
        RepositoryReadSource {
            registration: None,
            ..source()
        },
        RepositoryReadSource {
            repository: "https://secret@example.invalid/spec".into(),
            ..source()
        },
        RepositoryReadSource {
            start_commit: "HEAD".into(),
            ..source()
        },
    ] {
        cfg.instruction_source = Some(invalid);
        assert!(cfg.validate().is_err());
    }
}
