//! Join under tests/product.rs; exercises real product handlers and controller records.
use super::*;
use borg_runtime::collaboration::{documents::*,workflow::*};
use borg_runtime::collaboration::work::{WorkQuery,WorkQueryResult};

async fn workflow_fixture()->Fixture { workflow_fixture_with_page_items(4).await }
async fn workflow_fixture_with_page_items(page_items:usize)->Fixture {
    let root=std::env::temp_dir().join(format!("borg-workflow-product-{}-{}-{}",std::process::id(),now(),NEXT.fetch_add(1,Ordering::Relaxed)));
    std::fs::create_dir(&root).unwrap();let path=root.join("tasks.sqlite");
    let mut config=config();std::sync::Arc::make_mut(&mut config.limits).workflow=Some(borg_runtime::limits::WorkflowLimits {
        record_bytes:8192,text_bytes:2048,options:8,records:128,revisions:1024,page_items,page_bytes:16384,
        waits_per_task:8,graph_scan_nodes:128,events_per_mutation:16,event_bytes:4096,notice_bytes:4096,pending_delivery_bytes:16384,delivery_scan_rows:64,
    });
    let controller=Controller::open_with_config(&path,config).unwrap();Fixture{controller,path}
}
async fn document_read(c:&Controller,id:&str,version:Option<u64>)->DocumentView {
    match call(c,Operation::QueryDocument{query:DocumentQuery::Get{document_id:id.into(),version}}).await {
        Data::DocumentQuery(DocumentQueryResult::Document(v))=>v,x=>panic!("document expected {x:?}")
    }
}
async fn review_read(c:&Controller,id:&str)->ReviewView {
    match call(c,Operation::QueryReview{query:ReviewQuery::Get{review_id:id.into()}}).await {
        Data::ReviewQuery(ReviewQueryResult::Review(v))=>v,x=>panic!("review expected {x:?}")
    }
}

#[tokio::test]
//= spec/impl/v2/implementation.md#collaboration-surfaces-preserve-existing-authority
//= type=test
//# The browser interface MUST expose every supported collaboration operation permitted to its caller.
async fn browser_first_task_comment_accepts_zero_cas_and_refuses_stale_or_other_zero_revisions() {
    let f=workflow_fixture().await;
    let submitted=ui(&f.controller,"POST","/tasks",native_submission("browser-comment-task",now()+3_600_000,None)).await;
    let id=task_id(&submitted).to_owned();let before=task(&f.controller,&id).await;
    let Data::WorkQuery(WorkQueryResult::Task(before_work))=call(&f.controller,Operation::QueryWork{query:WorkQuery::Get{task_id:id.clone()}}).await else{panic!("initial work view expected")};
    assert_eq!(before_work.coordination_revision,0);
    // GetTask derives current admission diagnostics; WorkQuery retains the task.
    // Compare each complete projection with itself, rather than equating the two.
    assert_eq!(before_work.task.input,before.input);
    assert_eq!(before_work.task.revision,before.revision);
    assert_eq!(before_work.task.session_id,before.session_id);
    let route=format!("/tasks/{}/comments",encode(&id));
    let saved=ui(&f.controller,"POST",&route,form(&[("operation_id","browser-first-task-comment".into()),("expected_revision","0".into()),("body","First task comment".into())])).await;
    assert_eq!(saved.status,303,"{}",saved.body);
    let Data::WorkQuery(WorkQueryResult::Task(work))=call(&f.controller,Operation::QueryWork{query:WorkQuery::Get{task_id:id.clone()}}).await else{panic!("work view expected")};
    assert_eq!(work.coordination_revision,1);assert_eq!(work.task,before_work.task);
    assert_eq!(task(&f.controller,&id).await,before);
    let Data::DocumentQuery(DocumentQueryResult::Comments(comments))=call(&f.controller,Operation::QueryDocument{query:DocumentQuery::Comments{record:RecordRef::Task{task_id:id.clone()},after:None,limit:1}}).await else{panic!("comment expected")};
    assert_eq!(comments.items.len(),1);assert_eq!(comments.items[0].author.provenance,ActorProvenance::TrustedHost{path:TrustedHostPath::Browser});
    let stale=ui(&f.controller,"POST",&route,form(&[("operation_id","browser-stale-task-comment".into()),("expected_revision","0".into()),("body","Stale comment".into())])).await;
    assert_eq!(stale.status,409,"{}",stale.body);assert_eq!(task(&f.controller,&id).await,before);
    let malformed=ui(&f.controller,"POST",&route,form(&[("operation_id","browser-negative-task-comment".into()),("expected_revision","-1".into()),("body","Malformed".into())])).await;
    assert_eq!(malformed.status,400,"{}",malformed.body);
    let Data::DocumentMutation(DocumentMutationOutcome::Document{view:d})=call(&f.controller,Operation::MutateDocument{operation_id:"browser-zero-version-source".into(),mutation:DocumentMutation::Create{content:DocumentContent{title:"Source".into(),body:"Original".into()},supplied_content_id:None,responsible_owner:PrincipalRef::TrustedHostOperator}}).await else{panic!("document expected")};
    let refused=ui(&f.controller,"POST",&format!("/documents/{}/versions",encode(&d.document.id)),form(&[("operation_id","browser-zero-version".into()),("expected_revision","0".into()),("title","Invalid".into()),("body","Invalid".into())])).await;
    assert_eq!(refused.status,400,"{}",refused.body);
    let refused=ui(&f.controller,"POST","/reviews/review-example/decide",form(&[("operation_id","browser-zero-decision".into()),("expected_revision","0".into()),("verdict","approved".into()),("explanation","Invalid".into())])).await;
    assert_eq!(refused.status,400,"{}",refused.body);
    assert_eq!(document_read(&f.controller,&d.document.id,None).await,d);
}

#[tokio::test]
async fn browser_versions_and_comments_use_canonical_escaped_records_with_real_actor_origin() {
    let f=workflow_fixture().await;
    let create=ui(&f.controller,"POST","/documents",form(&[("operation_id","browser-document-create".into()),("title","Research <draft>".into()),("body","<script>inert</script> & original".into()),("owner_agent_id",String::new())])).await;
    assert_eq!(create.status,303,"{}",create.body);let route=create.location.unwrap();let id=route.strip_prefix("/documents/").unwrap();
    let first=document_read(&f.controller,id,None).await;
    assert_eq!(first.version.author.provenance,ActorProvenance::TrustedHost{path:TrustedHostPath::Browser});
    let view=ui(&f.controller,"GET",&route,vec![]).await;
    assert_eq!(view.status,200);assert!(view.body.contains("&lt;script&gt;inert&lt;/script&gt; &amp; original"));assert!(!view.body.contains("<script>inert"));
    let updated=ui(&f.controller,"POST",&format!("{route}/versions"),form(&[("operation_id","browser-document-edit".into()),("expected_revision",first.document.revision.to_string()),("title","Second".into()),("body","updated".into())])).await;
    assert_eq!(updated.status,303,"{}",updated.body);
    assert_eq!(document_read(&f.controller,id,Some(1)).await.version,first.version);
    let current=document_read(&f.controller,id,None).await;
    let stale=ui(&f.controller,"POST",&format!("{route}/versions"),form(&[("operation_id","browser-document-stale".into()),("expected_revision",first.document.revision.to_string()),("title","stale".into()),("body","overwrite".into())])).await;
    assert_eq!(stale.status,409,"{}",stale.body);assert_eq!(document_read(&f.controller,id,None).await,current);
    let comment=ui(&f.controller,"POST",&format!("{route}/comments"),form(&[("operation_id","browser-comment".into()),("expected_revision",current.document.revision.to_string()),("body","Keep <this> & attribution".into())])).await;
    assert_eq!(comment.status,303,"{}",comment.body);
    let comments=ui(&f.controller,"GET",&format!("{route}/comments"),vec![]).await;assert_eq!(comments.status,200);assert!(comments.body.contains("Keep &lt;this&gt; &amp; attribution"));
    let historical=ui(&f.controller,"GET",&format!("{route}?version=1"),vec![]).await;
    assert!(historical.body.contains("retained historical version"));assert!(!historical.body.contains("<button>Save new version</button>"));
}

#[tokio::test]
async fn browser_review_decision_retains_exact_target_and_rejects_an_injected_actor_field() {
    let f=workflow_fixture().await;
    let Data::DocumentMutation(DocumentMutationOutcome::Document{view:d})=call(&f.controller,Operation::MutateDocument{operation_id:"review-ui-source".into(),mutation:DocumentMutation::Create{content:DocumentContent{title:"Subject".into(),body:"One version".into()},supplied_content_id:None,responsible_owner:PrincipalRef::TrustedHostOperator}}).await else{panic!("document expected")};
    let target=ReviewTarget::DocumentVersion{version:d.version.reference};
    let create=ui(&f.controller,"POST","/reviews",form(&[("operation_id","browser-review-create".into()),("target",serde_json::to_string(&target).unwrap()),("reviewer_agent_id",String::new()),("summary","Is this complete <enough>?".into())])).await;
    assert_eq!(create.status,303,"{}",create.body);let route=create.location.unwrap();let id=route.strip_prefix("/reviews/").unwrap();let before=review_read(&f.controller,id).await;
    let injected=ui(&f.controller,"POST",&format!("{route}/decide"),form(&[("operation_id","browser-review-forged".into()),("expected_revision",before.review.revision.to_string()),("verdict","approved".into()),("explanation","good".into()),("actor","named-human".into())])).await;
    assert_eq!(injected.status,400);assert_eq!(review_read(&f.controller,id).await,before);
    let decide=ui(&f.controller,"POST",&format!("{route}/decide"),form(&[("operation_id","browser-review-decision".into()),("expected_revision",before.review.revision.to_string()),("verdict","changes_requested".into()),("explanation","Add the missing case".into())])).await;
    assert_eq!(decide.status,303,"{}",decide.body);let saved=review_read(&f.controller,id).await;
    let decision=saved.decision.unwrap();assert_eq!(decision.target,target);assert_eq!(decision.verdict,ReviewVerdict::ChangesRequested);assert_eq!(decision.reviewer.provenance,ActorProvenance::TrustedHost{path:TrustedHostPath::Browser});
    let view=ui(&f.controller,"GET",&route,vec![]).await;assert!(view.body.contains("Changes requested"));assert!(view.body.contains("do not authorize deployment"));assert!(!view.body.contains("<button>Record decision</button>"));
}

#[tokio::test]
async fn document_browser_mutations_preserve_existing_same_origin_guard() {
    let f=workflow_fixture().await;
    let request=UiRequest{method:"POST".into(),path:"/documents".into(),origin:Some("https://foreign.invalid".into()),expected_origin:ORIGIN.into(),body:form(&[("operation_id","cross-origin-document".into()),("title","untrusted".into()),("body","blocked".into())])};
    let rejected=handle(&f.controller,request).await;assert_eq!(rejected.status,403);
    match call(&f.controller,Operation::QueryDocument{query:DocumentQuery::List{after:None,limit:1}}).await {Data::DocumentQuery(DocumentQueryResult::Documents(page))=>assert!(page.items.is_empty()),x=>panic!("document list expected {x:?}")}
}

#[tokio::test]
// Scope: known child repository operands in an unaccepted browser draft. No
// claim about arbitrary secret detection, historical stores or identity grants.
//= spec/impl/v2/implementation.md#shared-product-state
//= type=test
//# The browser interface MUST present the information exposed through the shared agent interface.
async fn rejected_browser_workflow_draft_does_not_echo_credential_endpoints_and_keeps_safe_fields() {
    const MARKER: &str = "SYNTHETIC_BROWSER_CHILD_CREDENTIAL";
    let f = workflow_fixture().await;
    register_fixture_repository(&f.controller).await;
    let deadline = now() + 3_600_000;
    for which in ["url", "checkpoint_remote", "malformed"] {
        let mut draft = serde_json::to_value(workflow(deadline)).unwrap();
        let endpoint = if which == "checkpoint_remote" { "checkpoint_remote" } else { "url" };
        draft["child"]["repository"][endpoint] = serde_json::json!(format!("https://fixture-user:{MARKER}@example.invalid/repo.git"));
        let mut raw = serde_json::to_string(&draft).unwrap();
        if which == "malformed" { raw.push('{'); }
        let id = format!("browser-secret-refusal-{which}");
        let response = ui(&f.controller, "POST", "/tasks", native_submission(&id, deadline, Some(&raw))).await;
        assert_eq!(response.status, 400, "{}", response.body);
        assert!(!response.body.contains(MARKER), "refused endpoint must not be echoed into HTML");
        assert!(response.body.contains(&format!("name=\"operation_id\" value=\"{id}\"")));
        assert!(response.body.contains("Return the requested structured analysis."));
        assert!(response.body.contains("native-analysis"));
        assert!(response.body.contains("name=\"model_requests\" value=\"8\""));
        assert!(response.body.contains("name=\"native_workflow\" rows=\"8\"></textarea>"));
        assert!(response.body.contains(if which == "malformed" {
            "Invalid native workflow JSON or unknown configuration field"
        } else { "empty server-owned endpoints" }));
        let reply = f.controller.handle(Request { protocol_revision: WIRE_REVISION, request_id: format!("lookup-{which}"),
            command: Operation::GetOperation { operation_id: id } }).await;
        assert!(matches!(reply.outcome, Outcome::Refused { ref code, .. } if code == "not_found"));
    }
    // A safe malformed form keeps a fully decoded, credential-free workflow.
    let clean = serde_json::to_string(&workflow(deadline)).unwrap();
    let mut input = String::from_utf8(native_submission("browser-safe-redisplay", deadline, Some(&clean))).unwrap();
    input.push_str("&unknown_field=invalid");
    let response = ui(&f.controller, "POST", "/tasks", input.into_bytes()).await;
    assert_eq!(response.status, 400);
    assert!(response.body.contains("coding_then_continue"));
    assert!(response.body.contains("product-fixture-repository"));
    // The refused occurrence was never committed: the same ID can admit a
    // corrected request with its ordinary configured task rules unchanged.
    let accepted = ui(&f.controller, "POST", "/tasks", native_submission("browser-secret-refusal-url", deadline, Some(&clean))).await;
    assert_eq!(accepted.status, 303, "{}", accepted.body);
    let saved = task(&f.controller, task_id(&accepted)).await;
    assert!(!serde_json::to_string(&saved).unwrap().contains(MARKER));
    assert_eq!(saved.input.instruction, "Return the requested structured analysis.");
}

#[tokio::test]
//= spec/impl/v2/implementation.md#limits-are-versioned-metadata
//= type=test
//# An operation refused by a configured limit MUST identify the limiting metadata field.
//= spec/impl/v2/implementation.md#document-versions-and-comments-are-immutable
//= type=test
//# A document edit MUST create a new version without changing an earlier version.
async fn browser_document_utf8_limit_is_named_and_does_not_commit_a_partial_version() {
    let f = workflow_fixture().await;
    let initial = ui(&f.controller, "POST", "/documents", form(&[
        ("operation_id", "ui-byte-bound-create".into()), ("title", "Byte boundary".into()),
        ("body", "original".into()), ("owner_agent_id", String::new()),
    ])).await;
    assert_eq!(initial.status, 303, "{}", initial.body);
    let route = initial.location.unwrap(); let id = route.strip_prefix("/documents/").unwrap();
    let before = document_read(&f.controller, id, None).await;
    let maximum = f.controller.limits().workflow.as_ref().unwrap().text_bytes;
    assert_eq!(maximum % 2, 0);
    let fit = "é".repeat(maximum / 2); let over = format!("{fit}é");
    assert_eq!(fit.len(), maximum); assert!(over.chars().count() < maximum);
    let refused = ui(&f.controller, "POST", &format!("{route}/versions"), form(&[
        ("operation_id", "ui-byte-bound-too-long".into()), ("expected_revision", before.document.revision.to_string()),
        ("title", "UTF-8 successor".into()), ("body", over),
    ])).await;
    assert_eq!(refused.status, 409, "{}", refused.body);
    assert!(refused.body.contains("workflow.text_bytes"));
    assert_eq!(document_read(&f.controller, id, None).await, before);
    let accepted = ui(&f.controller, "POST", &format!("{route}/versions"), form(&[
        ("operation_id", "ui-byte-bound-exact".into()), ("expected_revision", before.document.revision.to_string()),
        ("title", "UTF-8 successor".into()), ("body", fit.clone()),
    ])).await;
    assert_eq!(accepted.status, 303, "{}", accepted.body);
    let after = document_read(&f.controller, id, None).await;
    assert_eq!(after.version.content.body, fit);
    assert_eq!(after.version.reference.version, 2);
    assert_eq!(document_read(&f.controller, id, Some(1)).await.version, before.version);
}

#[tokio::test]
//= spec/impl/v2/implementation.md#collaboration-surfaces-preserve-existing-authority
//= type=test
//# The browser interface MUST expose every supported collaboration operation permitted to its caller.
async fn empty_workflow_store_is_discoverable_from_existing_navigation_and_task_detail() {
    let f = workflow_fixture().await;
    let landing = ui(&f.controller, "GET", "/collaboration", vec![]).await;
    assert_eq!(landing.status, 200);
    for route in ["/documents", "/documents/new", "/questions", "/questions/new", "/reviews", "/decisions"] {
        assert!(landing.body.contains(&format!("href=\"{route}\"")), "missing supported entry {route}");
        let response = ui(&f.controller, "GET", route, vec![]).await;
        assert_eq!(response.status, 200, "{route}: {}", response.body);
    }
    let created = ui(&f.controller, "POST", "/tasks", native_submission("discoverable-coordination", now() + 3_600_000, None)).await;
    let id = task_id(&created);
    let task_page = ui(&f.controller, "GET", &format!("/tasks/{id}"), vec![]).await;
    for suffix in ["coordination", "comments"] {
        let route = format!("/tasks/{id}/{suffix}");
        assert!(task_page.body.contains(&format!("href=\"{route}\"")));
        let response = ui(&f.controller, "GET", &route, vec![]).await;
        assert_eq!(response.status, 200, "{}", response.body);
    }
    assert!(!task_page.body.contains("/review-target?scope="), "unaccepted work has no accepted review subject");
    // The ordinary historical profile remains a task-only view. No workflow
    // capability is silently enabled merely by adding navigation code.
    let old_root = std::env::temp_dir().join(format!("borg-old-navigation-{}-{}-{}",std::process::id(),now(),NEXT.fetch_add(1,Ordering::Relaxed)));
    std::fs::create_dir(&old_root).unwrap();
    let mut old_cfg = config();std::sync::Arc::make_mut(&mut old_cfg.limits).workflow=None;
    let old = Fixture { controller:Controller::open_with_config(old_root.join("tasks.sqlite"),old_cfg).unwrap(),path:old_root.join("tasks.sqlite") };
    let landing = ui(&old.controller, "GET", "/collaboration", vec![]).await;
    for route in ["/documents", "/questions", "/reviews", "/decisions"] {
        assert!(!landing.body.contains(&format!("href=\"{route}\"")));
    }
    let created = ui(&old.controller, "POST", "/tasks", native_submission("historical-task-navigation", now() + 3_600_000, None)).await;
    let page = ui(&old.controller, "GET", &format!("/tasks/{}", task_id(&created)), vec![]).await;
    assert!(!page.body.contains("aria-label=\"Task collaboration\""));
}

#[tokio::test]
//= spec/impl/v2/implementation.md#shared-product-state
//= type=test
//# The browser interface MUST present the information exposed through the shared agent interface.
//= spec/impl/v2/implementation.md#reviews-bind-the-exact-subject-version
//= type=test
//# A review decision MUST retain the reviewed subject identity.
async fn browser_exposes_exact_document_comment_and_closed_review_identities_as_escaped_data() {
    let f = workflow_fixture().await;
    let saved = ui(&f.controller, "POST", "/documents", form(&[
        ("operation_id", "exact-visible-document".into()), ("title", "Identity <title>".into()),
        ("body", "<script>never_execute()</script> & original".into()), ("owner_agent_id", String::new()),
    ])).await;
    let route = saved.location.unwrap(); let id = route.strip_prefix("/documents/").unwrap();
    let d = document_read(&f.controller, id, None).await;
    let page = ui(&f.controller, "GET", &route, vec![]).await;
    let details = page.body.split("<details id=\"document-record\">").nth(1).unwrap().split("</details>").next().unwrap();
    assert!(details.contains(&d.version.reference.content_id));
    assert!(details.contains("&quot;operation_id&quot;:&quot;exact-visible-document&quot;"));
    assert!(details.contains("&quot;product_revision&quot;:"));
    assert!(details.contains("&quot;path&quot;:&quot;browser&quot;"));
    assert!(details.contains("&lt;script&gt;never_execute()&lt;/script&gt; &amp; original"));
    assert!(!details.contains("<script>"));
    let Data::DocumentMutation(DocumentMutationOutcome::Comment { comment, .. }) = call(&f.controller, Operation::MutateDocument {
        operation_id: "exact-visible-comment".into(), mutation: DocumentMutation::PostComment {
            record: RecordRef::Document { document_id: id.into() }, expected_revision: d.document.revision,
            body: "Comment <tag> & retained".into(),
        },
    }).await else { panic!("expected comment") };
    let page = ui(&f.controller, "GET", &format!("{route}/comments"), vec![]).await;
    let details = page.body.split(&format!("<details id=\"comment-{}\">", comment.id)).nth(1).unwrap().split("</details>").next().unwrap();
    assert!(details.contains(&comment.id));
    assert!(details.contains("&quot;operation_id&quot;:&quot;exact-visible-comment&quot;"));
    assert!(details.contains("Comment &lt;tag&gt; &amp; retained"));
    assert!(details.contains("&quot;path&quot;:&quot;operator_api&quot;"));
    let target = ReviewTarget::DocumentVersion { version: d.version.reference.clone() };
    let Data::ReviewMutation(ReviewMutationOutcome::Review(review)) = call(&f.controller, Operation::MutateReview {
        operation_id: "exact-visible-review".into(), mutation: ReviewMutation::Request {
            target: target.clone(), reviewer: PrincipalRef::TrustedHostOperator, summary: "Review the original".into(),
        },
    }).await else { panic!("expected review") };
    let decided = ui(&f.controller, "POST", &format!("/reviews/{}/decide", review.review.id), form(&[
        ("operation_id", "exact-visible-decision".into()), ("expected_revision", review.review.revision.to_string()),
        ("verdict", "approved".into()), ("explanation", "Product decision <only>".into()),
    ])).await;
    assert_eq!(decided.status, 303, "{}", decided.body);
    let current = document_read(&f.controller, id, None).await;
    let updated = ui(&f.controller, "POST", &format!("{route}/versions"), form(&[
        ("operation_id", "exact-visible-later-version".into()), ("expected_revision", current.document.revision.to_string()),
        ("title", "New head".into()), ("body", "Different content".into()),
    ])).await;
    assert_eq!(updated.status, 303, "{}", updated.body);
    let page = ui(&f.controller, "GET", &format!("/reviews/{}", review.review.id), vec![]).await;
    assert!(page.body.contains(&format!("href=\"{route}?version=1\"")));
    let details = page.body.split("<details id=\"review-record\">").nth(1).unwrap().split("</details>").next().unwrap();
    assert!(details.contains(&d.version.reference.content_id));
    assert!(details.contains("&quot;operation_id&quot;:&quot;exact-visible-decision&quot;"));
    assert!(details.contains("&quot;verdict&quot;:&quot;approved&quot;"));
    assert!(details.contains("Product decision &lt;only&gt;"));
    assert!(!page.body.contains("<button>Record decision</button>"));
    let canonical = review_read(&f.controller, &review.review.id).await;
    assert_eq!(canonical.decision.unwrap().target, target);
}

#[tokio::test]
//= spec/impl/v2/implementation.md#limits-are-versioned-metadata
//= type=test
//# An operation refused by a configured limit MUST identify the limiting metadata field.
async fn canonical_detail_rendering_respects_the_complete_product_response_bound() {
    let f = workflow_fixture().await;
    let body = "&".repeat(f.controller.limits().workflow.as_ref().unwrap().text_bytes);
    let operation = Operation::MutateDocument { operation_id: "detail-response-bound-source".into(), mutation: DocumentMutation::Create {
        content: DocumentContent { title: "Bounded exact details".into(), body }, supplied_content_id: None,
        responsible_owner: PrincipalRef::TrustedHostOperator,
    } };
    let Data::DocumentMutation(DocumentMutationOutcome::Document { view }) = call(&f.controller, operation.clone()).await else { panic!("expected document") };
    let full = ui(&f.controller, "GET", &format!("/documents/{}", view.document.id), vec![]).await;
    assert_eq!(full.status, 200, "{}", full.body);
    let detail = full.body.split("<details id=\"document-record\">").nth(1).unwrap().split("</details>").next().unwrap();
    assert!(detail.len() > 1000, "fixture must discriminate the canonical details addition");
    let selected_bound = full.body.len() - detail.len() / 2;
    let mut cfg = config(); cfg.limits = std::sync::Arc::new(f.controller.limits().clone());
    let limits = std::sync::Arc::make_mut(&mut cfg.limits);
    limits.product.request_path_bytes = 512; // These test routes remain far below the selected path bound.
    limits.product.response_bytes = selected_bound;
    assert!(selected_bound >= limits.product.minimum_response_bytes(&limits.transport).unwrap());
    limits.validate().unwrap();
    let root = std::env::temp_dir().join(format!("borg-details-bound-{}-{}-{}", std::process::id(), now(), NEXT.fetch_add(1, Ordering::Relaxed)));
    std::fs::create_dir(&root).unwrap();
    let c = Controller::open_with_config(root.join("tasks.sqlite"), cfg).unwrap();
    let Data::DocumentMutation(DocumentMutationOutcome::Document { view: limited }) = call(&c, operation).await else { panic!("expected document") };
    let response = ui(&c, "GET", &format!("/documents/{}", limited.document.id), vec![]).await;
    assert_eq!(response.status, 502, "complete canonical rendering must not escape its source-selected byte bound");
    assert!(response.body.len() <= selected_bound);
    assert!(response.body.contains("product.response_bytes"));
    assert!(!response.body.contains("<details"), "a clipped partial record must not masquerade as a successful exact view");
    assert_eq!(document_read(&c, &limited.document.id, None).await, limited);
}

#[tokio::test]
//= spec/impl/v2/implementation.md#shared-collaboration-tool-interface
//= type=test
//# The collaboration tool interface MUST use the controller's canonical operation layer.
async fn original_work_receipt_uses_its_frozen_view_after_current_owner_changes() {
    use borg_runtime::collaboration::work::WorkMutation;
    let f=workflow_fixture().await;
    let submitted=ui(&f.controller,"POST","/tasks",native_submission("receipt-view-task",now()+3_600_000,None)).await;
    let task_id=task_id(&submitted).to_owned();let execution=task(&f.controller,&task_id).await;
    let operation=Operation::MutateWork {operation_id:"historical-work-view".into(),mutation:WorkMutation::ChangeOwner {
        task_id:task_id.clone(),expected_revision:0,owner:PrincipalRef::TrustedHostOperator,
    }};
    let Data::WorkMutation(original)=call(&f.controller,operation.clone()).await else{panic!("work mutation expected")};
    let Data::WorkMutation(current)=call(&f.controller,Operation::MutateWork {
        operation_id:"later-work-view".into(),mutation:WorkMutation::ChangeOwner {
            task_id:task_id.clone(),expected_revision:original.view.coordination_revision,
            owner:PrincipalRef::Agent{agent_id:execution.assigned_agent_id.clone()},
        },
    }).await else{panic!("later work mutation expected")};
    assert_eq!(current.view.coordination_revision,original.view.coordination_revision+1);
    assert_eq!(current.view.task,original.view.task);
    let page=ui(&f.controller,"GET","/operations/historical-work-view",vec![]).await;
    assert_eq!(page.status,200,"{}",page.body);
    assert!(page.body.contains("<main data-live-refresh=\"false\">"));
    assert!(page.body.contains("id=\"coordination-receipt\""));
    assert!(page.body.contains("Responsible owner at commit"));
    assert!(page.body.contains("Trusted host operator"));
    assert!(page.body.contains(&format!("&quot;coordination_revision&quot;: {}",original.view.coordination_revision)));
    assert!(!page.body.contains(&format!("&quot;coordination_revision&quot;: {}",current.view.coordination_revision)));
    assert!(!page.body.contains("<button type=\"submit\">Assign agent responsibility</button>"));
    assert_eq!(call(&f.controller,operation).await,Data::WorkMutation(original));
    let Data::WorkQuery(WorkQueryResult::Task(after))=call(&f.controller,Operation::QueryWork{query:WorkQuery::Get{task_id}}).await else{panic!("work view expected")};
    assert_eq!(after,current.view);
}

fn next_workflow_list_href(body:&str)->String {
    body.split("href=\"").filter_map(|tail|tail.split_once('"').map(|(href,_)|href))
        .find(|href|href.contains("after=")).expect("bounded list has a continuation").replace("&amp;","&")
}

#[tokio::test]
//= spec/impl/v2/implementation.md#collaboration-surfaces-preserve-existing-authority
//= type=test
//# The browser interface MUST expose every supported collaboration operation permitted to its caller.
//= spec/impl/v2/implementation.md#shared-product-state
//= type=test
//# The browser interface MUST present the information exposed through the shared agent interface.
async fn browser_question_and_review_filters_preserve_canonical_scope_through_continuation() {
    use borg_runtime::collaboration::questions::*;
    let f=workflow_fixture_with_page_items(1).await;
    let mut documents=Vec::new();
    for label in ["matching","other"] {
        let Data::DocumentMutation(DocumentMutationOutcome::Document{view})=call(&f.controller,Operation::MutateDocument {
            operation_id:format!("filter-doc-{label}"),mutation:DocumentMutation::Create {
                content:DocumentContent{title:label.into(),body:"Exact linked subject".into()},supplied_content_id:None,responsible_owner:PrincipalRef::TrustedHostOperator,
            },
        }).await else{panic!("document expected")};documents.push(view);
    }
    let mut questions=Vec::new();let mut reviews=Vec::new();
    for (label,index) in [("first",0),("second",0),("other",1),("closed",0)] {
        let document=&documents[index];
        let Data::QuestionMutation(value)=call(&f.controller,Operation::MutateQuestion {
            operation_id:format!("filter-question-{label}"),mutation:QuestionMutation::Pose {question:PoseQuestion {
                record:RecordRef::Document{document_id:document.document.id.clone()},recipient:PrincipalRef::TrustedHostOperator,
                prompt:format!("Choose the {label} outcome?"),options:vec![
                    QuestionOption{id:"keep".into(),label:"Keep".into(),explanation:"Retain this subject".into()},
                    QuestionOption{id:"change".into(),label:"Change".into(),explanation:"Prepare another subject".into()},
                ],recommended_option_id:"keep".into(),
            }},
        }).await else{panic!("question expected")};questions.push(value.question);
        let Data::ReviewMutation(ReviewMutationOutcome::Review(value))=call(&f.controller,Operation::MutateReview {
            operation_id:format!("filter-review-{label}"),mutation:ReviewMutation::Request {
                target:ReviewTarget::DocumentVersion{version:document.version.reference.clone()},reviewer:PrincipalRef::TrustedHostOperator,summary:format!("Review {label}"),
            },
        }).await else{panic!("review expected")};reviews.push(value);
    }
    call(&f.controller,Operation::MutateQuestion {operation_id:"filter-question-close".into(),mutation:QuestionMutation::Cancel {
        question_id:questions[3].question.id.clone(),expected_revision:questions[3].question.revision,reason:"This item is closed".into(),
    }}).await;
    call(&f.controller,Operation::MutateReview {operation_id:"filter-review-close".into(),mutation:ReviewMutation::Withdraw {
        review_id:reviews[3].review.id.clone(),expected_revision:reviews[3].review.revision,reason:"This item is closed".into(),
    }}).await;
    for (route,ids) in [("questions",questions.iter().map(|q|q.question.id.clone()).collect::<Vec<_>>()),
        ("reviews",reviews.iter().map(|r|r.review.id.clone()).collect::<Vec<_>>())] {
        let filter=format!("/{route}?record_kind=document&record_id={}&status=open",encode(&documents[0].document.id));
        // Questions use creation order; reviews scan bounded rows in opaque-ID
        // order and may return an empty filtered page with a continuation. The
        // browser must preserve the canonical sequence through all those pages.
        let record=Some(RecordRef::Document{document_id:documents[0].document.id.clone()});
        let mut after=None;
        let mut browser_route=filter.clone();
        let mut seen=std::collections::BTreeSet::new();
        let mut cursors=std::collections::BTreeSet::new();
        let mut finished=false;
        for page_number in 0..=ids.len() {
            let (expected_ids,next_cursor)=if route=="questions" {
                let Data::QuestionQuery(QuestionQueryResult::Questions(page))=call(&f.controller,Operation::QueryQuestion {
                    query:QuestionQuery::List{record:record.clone(),status:Some(QuestionStatus::Open),after:after.clone(),limit:1},
                }).await else{panic!("canonical question page expected")};
                (page.items.into_iter().map(|q|q.question.id).collect::<Vec<_>>(),page.next_cursor)
            } else {
                let Data::ReviewQuery(ReviewQueryResult::Reviews(page))=call(&f.controller,Operation::QueryReview {
                    query:ReviewQuery::List{record:record.clone(),status:Some(ReviewStatus::Open),after:after.clone(),limit:1},
                }).await else{panic!("canonical review page expected")};
                (page.items.into_iter().map(|review|review.id).collect::<Vec<_>>(),page.next_cursor)
            };
            assert!(expected_ids.len()<=1);
            let page=ui(&f.controller,"GET",&browser_route,vec![]).await;
            assert_eq!(page.status,200,"{route} page {page_number}: {}",page.body);
            for id in &ids {
                let visible=page.body.contains(&format!("href=\"/{route}/{}\"",encode(id)));
                assert_eq!(visible,expected_ids.contains(id),"{route} page {page_number}, {id}: {}",page.body);
            }
            for id in expected_ids {
                assert!(ids[..2].contains(&id),"wrong record or closed {route} leaked");
                assert!(seen.insert(id),"duplicate {route} item across pages");
            }
            let Some(cursor)=next_cursor else {
                assert!(!page.body.contains("after="));finished=true;break;
            };
            assert!(cursors.insert(cursor.clone()),"{route} cursor did not advance");
            let next=next_workflow_list_href(&page.body);
            assert!(next.contains("record_kind=document"));assert!(next.contains("status=open"));
            assert!(next.contains(&format!("record_id={}",encode(&documents[0].document.id))));
            assert!(next.ends_with(&format!("after={}",encode(&cursor))));
            browser_route=next;after=Some(cursor);
        }
        assert!(finished,"{route} pagination did not finish within all fixture rows");
        assert_eq!(seen,ids[..2].iter().cloned().collect(),"matching {route} omitted");
        for bad in [format!("/{route}?record_kind=document"),format!("/{route}?record_id=missing-kind"),format!("/{route}?status=unsupported")] {
            assert_eq!(ui(&f.controller,"GET",&bad,vec![]).await.status,400);
        }
    }
    let page=ui(&f.controller,"GET",&format!("/questions/{}",questions[0].question.id),vec![]).await;
    let exact=page.body.split("<details id=\"question-record\">").nth(1).unwrap().split("</details>").next().unwrap();
    assert!(exact.contains(&documents[0].document.id));
    assert!(exact.contains("&quot;record&quot;:"));
    assert!(exact.contains("&quot;recommended_option_id&quot;: &quot;keep&quot;"));
    assert!(exact.contains("&quot;path&quot;: &quot;operator_api&quot;"));
}
