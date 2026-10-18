//! Controlled pure fixtures only; no Git, HTTP, credentials or provider calls.
use super::*;
use crate::protocol::AttemptPhase;

fn limits() -> Bounds {
    Bounds { identity_bytes: 256, endpoint_bytes: 1024, ref_bytes: 2048,
        title_bytes: 256, body_bytes: 4096, record_bytes: 32768, lookup_rows: 4 }
}
fn target() -> Target {
    Target { revision: "target-one".into(), api_base: "https://api.github.com".into(),
        owner: "fixture-owner".into(), repository: "fixture-repository".into(), repository_id: 71,
        head_remote: "/controlled-fixture/head.git".into(), base_branch: "main".into(),
        credential_name: "GITHUB_TOKEN".into() }
}
fn request() -> Request { Request { title: "Fixture change".into(), body: "Exact requested body.\nSecond line.".into() } }
fn source() -> (Repository, Attempt, Checkpoint) {
    let registration = Revision { repository_id: "registered-fixture".into(), registration_revision: "one".into() };
    let a = Attempt { id: "attempt/one".into(), session_id: "session-one".into(), task_id: "task/one".into(),
        generation: 1, worker_id: "worker".into(), worker_incarnation: "incarnation".into(),
        phase: AttemptPhase::Running, deadline_unix_ms: 1000, stopped: false, uncertain: false };
    let cp = Checkpoint { registration: registration.clone(), repository: "/controlled-fixture/checkpoints.git".into(),
        commit: "a".repeat(40), remote_ref: format!("refs/heads/borg/tasks/{}/{}/{}",
            component(&a.task_id), component(&a.id), component("checkpoint-one")), operation_id: "checkpoint-one".into() };
    let r = Repository { registration: Some(registration), url: "/controlled-fixture/source.git".into(),
        start_commit: "b".repeat(40), checkpoint_remote: cp.repository.clone(), pull_request_target: Some(target()) };
    (r, a, cp)
}
fn head() -> HeadIntent {
    let (r, a, cp) = source();
    prepare_head(&target(), &r, &a, &cp, "original-pr-operation", &limits()).unwrap()
}
fn head_proof(h: &HeadIntent) -> HeadProof {
    HeadProof { operation_id: h.head_operation_id.clone(), target_revision: h.target.revision.clone(),
        remote: h.target.head_remote.clone(), remote_ref: h.head_ref.clone(), commit: h.binding.checkpoint.commit.clone() }
}
fn settled_head(h: &HeadIntent) -> OperationRecord {
    let mut r = head_record(h, &limits()).unwrap(); r.state = "settled".into();
    r.outcome = Some(json!(HeadOutcome::Applied(HeadSettlement { acknowledged: true, proof: head_proof(h) })));
    r
}
fn intent() -> Intent {
    let h = head(); prepare_pull_request(&h, &settled_head(&h), &request(), &limits()).unwrap()
}
fn proof(i: &Intent) -> Proof {
    let repository = |branch: String, commit: String| BranchProof { repository_id: i.head.target.repository_id,
        owner: i.head.target.owner.clone(), repository: i.head.target.repository.clone(), branch, commit };
    Proof { operation_id: i.head.binding.operation_id.clone(), provider_id: 901, number: 17,
        url: "https://github.com/fixture-owner/fixture-repository/pull/17".into(),
        head: repository(i.head.head_ref.strip_prefix("refs/heads/").unwrap().into(), i.head.binding.checkpoint.commit.clone()),
        base: repository(i.head.target.base_branch.clone(), "b".repeat(40)),
        title: i.title.clone(), body: i.body.clone(), marker: i.marker.clone() }
}

#[test]
fn only_an_explicit_request_requires_an_explicit_target() {
    let b = limits();
    assert_eq!(validate_selection(None, None, &b), Ok(()));
    assert_eq!(validate_selection(None, Some(&target()), &b), Ok(()));
    assert_eq!(validate_selection(Some(&request()), None, &b), Err(Error::Invalid("target_absent")));
    assert_eq!(validate_selection(Some(&request()), Some(&target()), &b), Ok(()));
}

#[test]
fn head_is_a_distinct_deterministic_effect_for_the_exact_accepted_source() {
    let b = limits(); let h = head();
    assert_eq!(h, head());
    assert!(h.head_ref.starts_with("refs/heads/borg/pr/"));
    assert_ne!(h.head_ref, h.binding.checkpoint.remote_ref);
    assert_ne!(h.head_operation_id, h.binding.operation_id);
    assert_ne!(h.head_operation_id, h.binding.checkpoint.operation_id);
    let (mut r, a, mut cp) = source();
    let other = prepare_head(&target(), &r, &a, &cp, "another-original", &b).unwrap();
    assert_ne!(h.head_ref, other.head_ref);
    assert_ne!(h.head_operation_id, other.head_operation_id);
    cp.commit = "c".repeat(40);
    assert_ne!(h.head_ref, prepare_head(&target(), &r, &a, &cp, "original-pr-operation", &b).unwrap().head_ref);
    r.pull_request_target.as_mut().unwrap().revision = "new-target".into();
    assert!(prepare_head(&target(), &r, &a, &cp, "original-pr-operation", &b).is_err());
    let (r, a, mut cp) = source(); cp.registration.registration_revision = "other".into();
    assert!(prepare_head(&target(), &r, &a, &cp, "original-pr-operation", &b).is_err());
    let mut forged = h.clone(); forged.head_ref.push_str("-foreign");
    assert!(validate_head_intent(&forged, &b).is_err());
    forged = h; forged.binding.checkpoint.remote_ref = "refs/heads/main".into();
    assert!(validate_head_intent(&forged, &b).is_err());
}

#[test]
fn only_the_exact_canonical_settled_head_unlocks_create_intent() {
    let b = limits(); let h = head(); let r = settled_head(&h);
    let i = prepare_pull_request(&h, &r, &request(), &b).unwrap();
    assert_eq!(i.head_proof, head_proof(&h));
    for state in ["pending", "uncertain"] {
        let mut altered = r.clone(); altered.state = state.into();
        assert!(prepare_pull_request(&h, &altered, &request(), &b).is_err());
    }
    let alterations: Vec<fn(&mut OperationRecord)> = vec![
        |r| r.id.push_str("-foreign"), |r| r.task_id.push_str("-foreign"),
        |r| r.attempt_id.push_str("-foreign"), |r| r.generation += 1,
        |r| r.kind = "checkpoint".into(),
        |r| r.intended["head_ref"] = json!("refs/heads/foreign"),
        |r| r.outcome = Some(json!({"acknowledged":true})),
        |r| r.outcome.as_mut().unwrap()["acknowledged"] = json!(false),
        |r| r.outcome.as_mut().unwrap()["proof"]["commit"] = json!("c".repeat(40)),
        |r| r.outcome.as_mut().unwrap()["extra"] = json!(true),
    ];
    for alter in alterations {
        let mut altered = r.clone(); alter(&mut altered);
        assert!(prepare_pull_request(&h, &altered, &request(), &b).is_err());
    }
}

#[test]
fn accepted_checkpoint_endpoints_preserve_existing_git_transport_identities() {
    let b = limits();
    for endpoint in [
        "git://127.0.0.1:19418/runtime.git",
        "git://[::1]:19418/runtime.git",
        "file:///controlled-fixture/checkpoints.git",
        "https://example.invalid/checkpoints.git",
        "ssh://git@example.invalid/checkpoints.git",
        "git+ssh://git@example.invalid/checkpoints.git",
        "ssh+git://git@example.invalid/checkpoints.git",
        "git@example.invalid:checkpoints.git",
        "/controlled-fixture/checkpoints.git",
        "../controlled-fixture/checkpoints.git",
    ] {
        let (mut r, a, mut cp) = source();
        r.checkpoint_remote = endpoint.into();
        cp.repository = endpoint.into();
        let h = prepare_head(&target(), &r, &a, &cp, "original-pr-operation", &b).unwrap();
        assert_eq!(h.binding.checkpoint, cp);
        assert_eq!(h.target.head_remote, target().head_remote);
        let i = prepare_pull_request(&h, &settled_head(&h), &request(), &b).unwrap();
        assert_eq!(i.head.binding.checkpoint.repository, endpoint);
        assert_eq!(i.head_proof.remote, target().head_remote);

        // A different accepted checkpoint identity is still not interchangeable.
        cp.repository.push_str("-foreign");
        assert_eq!(prepare_head(&target(), &r, &a, &cp, "original-pr-operation", &b),
            Err(Error::Mismatch("accepted_repository")));
    }
}

#[test]
fn accepted_checkpoint_endpoint_checks_keep_credentials_controls_and_bounds_closed() {
    let b = limits();
    for endpoint in [
        "", " \t", "git://127.0.0.1/runtime.git\n",
        "git://synthetic-token@127.0.0.1/runtime.git",
        "https://synthetic-token@example.invalid/checkpoints.git",
        "ssh://git:synthetic-password@example.invalid/checkpoints.git",
        "git+ssh://git:synthetic-password@example.invalid/checkpoints.git",
    ] {
        let (mut r, a, mut cp) = source();
        r.checkpoint_remote = endpoint.into();
        cp.repository = endpoint.into();
        assert_eq!(prepare_head(&target(), &r, &a, &cp, "original-pr-operation", &b),
            Err(Error::Invalid("checkpoint_repository")));
    }
    let (mut r, a, mut cp) = source();
    cp.repository = "x".repeat(b.endpoint_bytes + 1);
    r.checkpoint_remote = cp.repository.clone();
    assert_eq!(prepare_head(&target(), &r, &a, &cp, "original-pr-operation", &b),
        Err(Error::Bound("checkpoint_repository")));
}

#[test]
fn accepted_checkpoint_transport_does_not_expand_configured_head_authority() {
    let b = limits();
    for endpoint in ["git://127.0.0.1:19418/runtime.git", "file:///controlled-fixture/head.git",
        "git+ssh://git@example.invalid/head.git", "../controlled-fixture/head.git"] {
        let mut changed = target();
        changed.head_remote = endpoint.into();
        assert_eq!(validate_target(&changed, &b), Err(Error::Invalid("head_remote")));
    }
    let (mut r, a, mut cp) = source();
    cp.repository = "git://127.0.0.1:19418/runtime.git".into();
    r.checkpoint_remote = cp.repository.clone();
    let h = prepare_head(&target(), &r, &a, &cp, "original-pr-operation", &b).unwrap();
    let mut wrong = head_proof(&h);
    wrong.remote = cp.repository;
    assert_eq!(validate_head_proof(&h, &wrong, &b), Err(Error::Mismatch("head_proof")));
}

#[test]
fn definite_not_applied_closes_effect_but_never_supplies_requested_result() {
    let b = limits(); let h = head(); let negative = NotApplied { acknowledged: true, not_applied: true };
    let head_outcome = HeadOutcome::NotApplied(negative.clone());
    assert_eq!(serde_json::to_value(&head_outcome).unwrap(), json!({"acknowledged":true,"not_applied":true}));
    assert!(validate_head_outcome(&h, &head_outcome, &b).is_ok());
    let mut r = settled_head(&h); r.outcome = Some(json!(head_outcome));
    assert_eq!(prepare_pull_request(&h, &r, &request(), &b), Err(Error::Unsettled("head_not_applied")));
    let i = intent(); let outcome = CreateOutcome::NotApplied(negative);
    assert_eq!(validate_outcome(&i, &outcome, &b), Ok(None));
    let mut r = intent_record(&i, &b).unwrap(); r.state = "settled".into(); r.outcome = Some(json!(outcome));
    assert_eq!(reference_from_record(&r, &b), Err(Error::Unsettled("pr_not_applied")));
    for bad in [json!({"acknowledged":true}), json!({"acknowledged":true,"not_applied":true,"proof":head_proof(&h)}),
        json!({"acknowledged":true,"not_applied":true,"extra":true})] {
        assert!(serde_json::from_value::<HeadOutcome>(bad.clone()).is_err());
        assert!(serde_json::from_value::<CreateOutcome>(bad).is_err());
    }
    for (acknowledged, not_applied) in [(false, true), (true, false), (false, false)] {
        assert!(validate_head_outcome(&h, &HeadOutcome::NotApplied(NotApplied { acknowledged, not_applied }), &b).is_err());
        assert!(validate_outcome(&i, &CreateOutcome::NotApplied(NotApplied { acknowledged, not_applied }), &b).is_err());
    }
}

#[test]
fn create_body_uses_branches_and_original_marker_without_idempotency_claim() {
    let b = limits(); let i = intent(); let body = create_body(&i, &b).unwrap();
    assert_eq!(body["head"], i.head.head_ref.strip_prefix("refs/heads/").unwrap());
    assert_eq!(body["base"], "main");
    assert_ne!(body["head"], i.head.binding.checkpoint.commit);
    assert_eq!(body["title"], request().title);
    assert_eq!(body["body"], format!("{}\n\n{}", request().body, i.marker));
    assert_eq!(body["maintainer_can_modify"], false);
    assert_eq!(body.as_object().unwrap().len(), 5);
    assert!(body.get("idempotency_key").is_none());
    assert!(validate_requested_result(&i, &request(), &b).is_ok());
    let mut changed = request(); changed.body.push('!');
    assert!(validate_requested_result(&i, &changed, &b).is_err());
}

#[test]
fn provider_proof_rejects_every_wrong_identity_and_exact_source_substitution() {
    let b = limits(); let i = intent(); let p = proof(&i);
    let result = validate_proof(&i, &p, &b).unwrap();
    assert_eq!(result.operation_id, i.head.binding.operation_id);
    assert_eq!(result.head_operation_id, i.head.head_operation_id);
    assert_eq!(result.checkpoint_operation_id, i.head.binding.checkpoint.operation_id);
    assert_eq!(result.head, p.head); assert_eq!(result.base, p.base);
    let changes: Vec<fn(&mut Proof)> = vec![
        |p| p.operation_id.push_str("-foreign"), |p| p.provider_id = 0, |p| p.number = 0,
        |p| p.head.repository_id += 1, |p| p.base.repository_id += 1,
        |p| p.head.owner = "foreign".into(), |p| p.base.owner = "foreign".into(),
        |p| p.head.repository = "foreign".into(), |p| p.base.repository = "foreign".into(),
        |p| p.head.branch = "main".into(), |p| p.base.branch = "other-base".into(),
        |p| p.head.commit = "f".repeat(40), |p| p.base.commit = "not-a-sha".into(),
        |p| p.url = "https://foreign.invalid/fixture-owner/fixture-repository/pull/17".into(),
        |p| p.url = "https://github.com/fixture-owner/fixture-repository/pull/18".into(),
        |p| p.url.push_str("?token=synthetic"), |p| p.title.push('!'),
        |p| p.body.push('!'), |p| p.marker.clear(),
    ];
    for change in changes { let mut changed = p.clone(); change(&mut changed); assert!(validate_proof(&i, &changed, &b).is_err()); }
}

#[test]
fn normal_base_advancement_is_observed_without_retargeting_base_branch() {
    let b = limits(); let i = intent(); let mut p = proof(&i);
    p.base.commit = "c".repeat(40);
    let reference = validate_proof(&i, &p, &b).unwrap();
    assert_eq!(reference.base.commit, "c".repeat(40));
    assert_eq!(reference.base.branch, "main");
    assert_eq!(reference.head.commit, i.head.binding.checkpoint.commit);
    p.base.branch = "other".into(); assert!(validate_proof(&i, &p, &b).is_err());
}

#[test]
fn result_reference_is_derived_only_from_the_exact_settled_create_record() {
    let b = limits(); let i = intent(); let p = proof(&i);
    let mut r = intent_record(&i, &b).unwrap();
    r.outcome = Some(json!(CreateOutcome::Applied(Settlement { acknowledged: true, proof: p.clone() })));
    assert!(reference_from_record(&r, &b).is_err());
    r.state = "settled".into();
    assert_eq!(reference_from_record(&r, &b).unwrap(), validate_proof(&i, &p, &b).unwrap());
    r.generation += 1; assert!(reference_from_record(&r, &b).is_err()); r.generation -= 1;
    r.outcome = Some(json!({"acknowledged":true})); assert!(reference_from_record(&r, &b).is_err());
}

#[test]
fn incomplete_absent_ambiguous_or_foreign_observation_never_allows_repetition() {
    let b = limits(); let i = intent(); let p = proof(&i);
    assert_eq!(reconcile(&i, &[], true, &b), Ok(Reconciliation::Unknown));
    assert_eq!(reconcile(&i, std::slice::from_ref(&p), false, &b), Ok(Reconciliation::Unknown));
    assert_eq!(reconcile(&i, std::slice::from_ref(&p), true, &b), Ok(Reconciliation::Observed(p.clone())));
    assert_eq!(reconcile(&i, &[p.clone(), p.clone()], true, &b), Err(Error::Collision));
    let mut foreign = p.clone(); foreign.marker = "foreign".into();
    assert!(reconcile(&i, &[foreign], true, &b).is_err());
    assert_eq!(reconcile(&i, &vec![p; b.lookup_rows + 1], true, &b), Err(Error::Bound("lookup_rows")));
}

#[test]
fn durable_json_is_closed_including_legacy_checkpoint_and_nested_provider_proofs() {
    let b = limits(); let h = head(); let i = intent(); let p = proof(&i);
    assert_eq!(decode_head_intent(&serde_json::to_vec(&h).unwrap(), &b).unwrap(), h);
    assert_eq!(decode_intent(&serde_json::to_vec(&i).unwrap(), &b).unwrap(), i);
    for field in ["unknown", "authorization"] {
        let mut v = json!(h); v[field] = json!(true);
        assert!(decode_head_intent(&serde_json::to_vec(&v).unwrap(), &b).is_err());
    }
    let mut v = json!(h); v["binding"]["checkpoint"]["unknown"] = json!(true);
    assert!(decode_head_intent(&serde_json::to_vec(&v).unwrap(), &b).is_err());
    let mut v = json!(h); v["binding"]["checkpoint"]["registration"]["unknown"] = json!(true);
    assert!(decode_head_intent(&serde_json::to_vec(&v).unwrap(), &b).is_err());
    let mut v = json!(p); v["head"]["unknown"] = json!(true);
    assert!(decode_proof(&serde_json::to_vec(&v).unwrap(), &b).is_err());
    let mut v = json!(target()); v["expected_base_commit"] = json!("a".repeat(40));
    assert!(serde_json::from_value::<Target>(v).is_err());
    assert!(decode_intent(b"{\"head\":null}", &b).is_err());
}

#[test]
fn utf8_text_marker_and_complete_encoded_record_bounds_are_enforced() {
    let h = head(); let r = settled_head(&h); let mut b = limits();
    let request = Request { title: "éé".into(), body: String::new() };
    b.title_bytes = 4;
    let i = prepare_pull_request(&h, &r, &request, &b).unwrap();
    b.title_bytes = 3; assert_eq!(validate_request(&request, &b), Err(Error::Bound("title")));
    b = limits(); b.body_bytes = i.body.len();
    assert!(prepare_pull_request(&h, &r, &request, &b).is_ok());
    b.body_bytes -= 1; assert_eq!(prepare_pull_request(&h, &r, &request, &b), Err(Error::Bound("body")));
    let mut spoof = request.clone(); spoof.body = format!("text {MARKER_PREFIX}foreign -->");
    assert_eq!(validate_request(&spoof, &limits()), Err(Error::Invalid("reserved_marker")));
    b = limits(); let encoded = serde_json::to_vec(&i).unwrap(); b.record_bytes = encoded.len();
    assert_eq!(decode_intent(&encoded, &b).unwrap(), i);
    b.record_bytes -= 1; assert_eq!(decode_intent(&encoded, &b), Err(Error::Bound("record_bytes")));
    let escaped = Request { title: "quotation: \" and slash: \\".into(), body: "\n\n".into() };
    b = limits(); b.record_bytes = serde_json::to_vec(&escaped).unwrap().len() - 1;
    assert_eq!(validate_request(&escaped, &b), Err(Error::Bound("record_bytes")));
}

#[test]
fn target_contains_configuration_names_and_explicit_destinations_only() {
    let b = limits(); assert!(validate_target(&target(), &b).is_ok());
    let changes: Vec<fn(&mut Target)> = vec![
        |t| t.api_base = "https://synthetic-secret@api.github.com".into(),
        |t| t.api_base = "https://api.github.com?credential=synthetic".into(),
        |t| t.api_base = "http://public.invalid".into(),
        |t| t.head_remote = "origin".into(),
        |t| t.head_remote = "https://user:synthetic-secret@github.com/a/b.git".into(),
        |t| t.head_remote = "ext::arbitrary-helper".into(),
        |t| t.repository_id = 0,
        |t| t.repository = "../foreign".into(),
        |t| t.base_branch = "a".repeat(40),
        |t| t.base_branch = "refs/heads/main".into(),
        |t| t.base_branch = "main.lock".into(),
        |t| t.base_branch = "main..foreign".into(),
        |t| t.credential_name = "TOKEN=synthetic-secret".into(),
        |t| t.credential_name = "ARBITRARY_ENVIRONMENT_NAME".into(),
    ];
    for change in changes { let mut t = target(); change(&mut t); assert!(validate_target(&t, &b).is_err()); }
    let mut t = target(); t.api_base = "http://127.0.0.1:12345".into();
    assert!(validate_target(&t, &b).is_ok());
}

#[test]
fn bound_projection_uses_existing_metadata_without_new_defaults() {
    let mut source = crate::limits::RuntimeLimits::seed().unwrap();
    source.repository.endpoint_bytes = 103;
    source.controller.max_repository_identity_bytes = 101;
    let b = Bounds::from_limits(&source);
    assert_eq!(b.endpoint_bytes, 101);
    assert_eq!(b.identity_bytes, source.transport.identity_bytes);
    assert_eq!(b.ref_bytes, source.controller.max_checkpoint_ref_bytes);
    assert_eq!(b.title_bytes, source.controller.max_progress_bytes);
    assert_eq!(b.body_bytes, source.controller.effect_value_bytes);
    assert_eq!(b.record_bytes, source.controller.effect_value_bytes);
    assert_eq!(b.lookup_rows, source.controller.max_list_tasks);
    let mut invalid = limits(); invalid.lookup_rows = 0;
    assert_eq!(validate_selection(None, None, &invalid), Err(Error::Invalid("bounds")));
}

#[test]
fn provider_url_rejects_equivalent_noncanonical_evidence_without_rewriting_it() {
    let b = limits(); let i = intent(); let original = proof(&i);
    assert_eq!(validate_proof(&i, &original, &b).unwrap().url, original.url);
    for raw in [
        "https://github.com/fixture-owner/extra/../fixture-repository/pull/17",
        "https://github.com/fixture-owner/./fixture-repository/pull/17",
        "https://github.com:443/fixture-owner/fixture-repository/pull/17",
        "https://GITHUB.COM/fixture-owner/fixture-repository/pull/17",
        "HTTPS://github.com/fixture-owner/fixture-repository/pull/17",
    ] {
        assert_eq!(reqwest::Url::parse(raw).unwrap().as_str(), original.url);
        let mut altered = original.clone(); altered.url = raw.into();
        assert_eq!(validate_proof(&i, &altered, &b), Err(Error::Mismatch("provider_url")));
        assert_eq!(altered.url, raw, "validation must not rewrite provider evidence");
    }
}

#[test]
fn old_absent_optional_fields_decode_and_reencode_without_new_values() {
    let repository = json!({"registration":{"repository_id":"fixture","registration_revision":"one"},
        "url":"/fixture/source.git","start_commit":"a".repeat(40),"checkpoint_remote":"/fixture/cp.git"});
    let decoded: Repository = serde_json::from_value(repository.clone()).unwrap();
    assert!(decoded.pull_request_target.is_none());
    assert_eq!(serde_json::to_value(decoded).unwrap(), repository);
    let registration = json!({"repository_id":"fixture","registration_revision":"one","display_name":"Fixture",
        "source_endpoint":"/fixture/source.git","checkpoint_endpoint":"/fixture/cp.git", "source_store_key":"source.git","checkpoint_store_key":"cp.git"});
    let decoded: crate::repository_catalogue::Registration = serde_json::from_value(registration.clone()).unwrap();
    assert!(decoded.pull_request_target.is_none());
    assert_eq!(serde_json::to_value(decoded).unwrap(), registration);
    let result = json!({"attempt_id":"attempt","summary":"historical checkpoint-only result","checkpoint":null,"validation":[],"output":null});
    let decoded: crate::protocol::AcceptedResult = serde_json::from_value(result.clone()).unwrap();
    assert!(decoded.pull_request.is_none());
    assert_eq!(serde_json::to_value(decoded).unwrap(), result);
    let definition = json!({"id":"fixture","revision":"one","executor":"coding"});
    let limits = json!({"deadline_unix_ms":1000,"model_requests":1,"output_bytes":256,
        "result_bytes":2048,"max_delegation_depth":0});
    let input = json!({"instruction":"historical instruction","definition":definition,
        "repository":repository,"acceptance_commands":[],"output_schema":null,
        "limits":limits,"dependencies":[]});
    let decoded: crate::protocol::TaskInput = serde_json::from_value(input.clone()).unwrap();
    assert!(decoded.pull_request.is_none());
    assert_eq!(serde_json::to_value(decoded).unwrap(), input);
    let template = json!({"instruction_prefix":"historical prefix","definition":definition,
        "repository":repository,"acceptance_commands":[],"limits":limits,"dependencies":[]});
    let decoded: crate::protocol::CodingTaskTemplate = serde_json::from_value(template.clone()).unwrap();
    assert!(decoded.pull_request.is_none());
    assert_eq!(serde_json::to_value(decoded).unwrap(), template);
}


#[test]
fn public_catalogue_only_advertises_availability_and_preserves_old_absence() {
    let old = json!({"repository_id":"fixture","registration_revision":"one","display_name":"Fixture",
        "source_endpoint":"source","checkpoint_endpoint":"checkpoint","current_revision":"one",
        "generation":1,"enabled_for_new_tasks":true});
    let mut entry: crate::repository_catalogue::Entry = serde_json::from_value(old.clone()).unwrap();
    assert!(!entry.pull_request_available);
    assert_eq!(serde_json::to_value(&entry).unwrap(), old);
    entry.pull_request_available = true;
    let public = serde_json::to_value(entry).unwrap();
    assert_eq!(public["pull_request_available"], true);
    assert!(public.get("pull_request_target").is_none());
    assert!(public.get("credential_name").is_none());
}

#[test]
fn delegated_child_copies_explicit_request_without_instruction_inference() {
    use crate::protocol::{TaskInput, Definition, ExecutorKind, InferenceSettings,
        ExecutionProfile, Limits, CodingTaskTemplate, NativeWorkflow};
    let definition = |id: &str, executor| Definition { charter: None, limit_overrides: None,
        execution_profile: Some(ExecutionProfile::CollaborationV1 { web_search: None }),
        inference: Some(InferenceSettings::selected()), id: id.into(), revision: "one".into(), executor };
    let runtime = crate::limits::RuntimeLimits::seed().unwrap();
    let (repository, _, _) = source();
    let configured = request();
    let mut parent = TaskInput { instruction_source: None, pull_request: None, resolved_limits: None, repository_read: None,
        instruction: "Delegate the configured coding work.".into(), definition: definition("parent", ExecutorKind::Native),
        repository: None, acceptance_commands: vec![], output_schema: Some(json!({"type":"string"})),
        limits: Limits { deadline_unix_ms: 10_000, model_requests: 3, output_bytes: 4096,
            result_bytes: runtime.task_defaults.result_bytes, max_delegation_depth: 2 }, dependencies: vec![],
        native_workflow: Some(Box::new(NativeWorkflow::CodingThenContinue { child: CodingTaskTemplate { instruction_source: None,
            pull_request: Some(configured.clone()), instruction_prefix: "Preserve configuration.".into(),
            definition: definition("child", ExecutorKind::Coding), repository, acceptance_commands: vec![],
            dependencies: vec![], limits: Limits { deadline_unix_ms: 9_000, model_requests: 1,
                output_bytes: 2048, result_bytes: 8192, max_delegation_depth: 1 },
        } })) };
    parent.resolved_limits = Some(runtime.resolve_task(&parent).unwrap());
    let child = crate::delegation::coding_child(&parent, "Change the requested code; title=untrusted").unwrap();
    assert_eq!(child.pull_request, Some(configured));
    let NativeWorkflow::CodingThenContinue { child } = parent.native_workflow.as_deref_mut().unwrap() else { unreachable!() };
    child.pull_request = None;
    let child = crate::delegation::coding_child(&parent, "Please open a pull request").unwrap();
    assert!(child.pull_request.is_none());
}
