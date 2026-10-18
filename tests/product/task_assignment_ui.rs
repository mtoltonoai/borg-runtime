//! Rendered assignment form -> existing Browser route -> canonical receipts.
//! No executor/provider process; the pending case uses controlled worker commands.
use super::*;
use borg_runtime::collaboration::{Query, QueryResult};

fn route(id: &str) -> String { format!("/tasks/{}", encode(id)) }
fn assignment_form(page: &UiResponse, id: &str) -> String {
    assert_eq!(page.status, 200, "{}", page.body);
    let start = format!("<form method=\"post\" action=\"{}/assign\">", route(id));
    assert_eq!(page.body.matches(&start).count(), 1);
    page.body.split_once(&start).unwrap().1.split_once("</form>").unwrap().0.into()
}
fn value(form: &str, name: &str) -> String {
    let tail = form.split_once(&format!("name=\"{name}\"")).unwrap().1;
    tail.split_once("value=\"").unwrap().1.split_once('"').unwrap().0.into()
}
fn operands(rendered: &str, agent: &str) -> Vec<u8> {
    form(&[("operation_id", value(rendered,"operation_id")),
        ("expected_revision", value(rendered,"expected_revision")), ("agent_id",agent.into())])
}
async fn original(c: &Controller, id: &str) -> Reply {
    let op = Operation::GetOperation { operation_id:id.into() };
    let r = Box::pin(c.handle(Request {protocol_revision:WIRE_REVISION,
        request_id:format!("assignment-ui-lookup-{}",NEXT.fetch_add(1,Ordering::Relaxed)),command:op.clone()})).await;
    r.validate_for(&op).unwrap(); r
}
async fn lineage(c: &Controller, id: &str) -> (Session,TaskInput,bool) {
    match call(c,Operation::QueryCollaboration {query:Query::SessionInput {session_id:id.into()}}).await {
        Data::Collaboration(QueryResult::SessionInput {session,input,current}) => (session,*input,current),
        other => panic!("session input {other:?}"),
    }
}
async fn sessions(c: &Controller) -> Vec<Session> {
    match call(c,Operation::QueryCollaboration {query:Query::Sessions {agent_id:None,after:None,limit:10}}).await {
        Data::Collaboration(QueryResult::Sessions(p)) => {assert!(p.next_cursor.is_none());p.items},
        other => panic!("sessions {other:?}"),
    }
}
async fn target(c: &Controller, initial: &Task, id: &str) -> Definition {
    let mut d=initial.input.definition.clone();d.id=id.into();d.revision="assignment-target-r1".into();
    assert!(matches!(call(c,Operation::RegisterAgent {operation_id:format!("register-{id}"),definition:d.clone()}).await,
        Data::Agent(a) if a.definition==d && !a.retired));d
}
async fn owner_closed(path: &std::path::Path) {
    let mut lock=path.as_os_str().to_os_string();lock.push(".controller-lock");
    tokio::time::timeout(std::time::Duration::from_secs(3),async {
        loop {let f=std::fs::OpenOptions::new().read(true).write(true).open(&lock).unwrap();
            if f.try_lock().is_ok(){break;}tokio::time::sleep(std::time::Duration::from_millis(5)).await;}
    }).await.expect("original controller owner closes before reopen");
}
async fn seed(label: &str) -> (Fixture,Task,Definition) {
    let f=Fixture::new().await;
    let accepted=ui(&f.controller,"POST","/tasks",submission(label,"Keep <original> work and its obligations","")).await;
    assert_eq!(accepted.status,303,"{}",accepted.body);
    let t=task(&f.controller,task_id(&accepted)).await;
    let d=target(&f.controller,&t,"assignment-target").await;(f,t,d)
}

#[tokio::test]
async fn task_assignment_ui_rendered_form_preserves_lineage_original_replay_and_restart() {
    Box::pin(rendered_assignment_journey()).await;
}
async fn rendered_assignment_journey() {
    let (f,initial,d)=seed("assignment-ui-source").await;
    let page=ui(&f.controller,"GET",&route(&initial.id),vec![]).await;
    assert!(page.body.contains("preserving its work, deadline and prior sessions"));
    assert!(!page.body.contains("Create a new session to choose a different agent"));
    let rendered=assignment_form(&page,&initial.id);
    assert!(rendered.contains("Target agent ID") && rendered.contains("required"));
    assert_eq!(value(&rendered,"agent_id"),"");assert_eq!(value(&rendered,"expected_revision"),initial.revision.to_string());
    let id=value(&rendered,"operation_id");assert!(!id.is_empty());
    let body=operands(&rendered,&d.id);
    let sent=ui(&f.controller,"POST",&format!("{}/assign",route(&initial.id)),body.clone()).await;
    assert_eq!(sent.status,303,"{}",sent.body);assert_eq!(sent.location,Some(route(&initial.id)));
    let next=task(&f.controller,&initial.id).await;
    assert_eq!(next.id,initial.id);assert_ne!(next.session_id,initial.session_id);assert_eq!(next.input.definition,d);
    assert_eq!(next.input.instruction,initial.input.instruction);assert_eq!(next.input.limits,initial.input.limits);
    assert_eq!(next.input.dependencies,initial.input.dependencies);assert_eq!(next.input.output_schema,initial.input.output_schema);
    assert_eq!(next.input.repository,initial.input.repository);assert_eq!(next.input.acceptance_commands,initial.input.acceptance_commands);
    assert!(next.attempt.is_none() && next.pending_assignment.is_none());assert_eq!(next.status,TaskStatus::Queued);
    let old=lineage(&f.controller,&initial.session_id).await;assert!(!old.2);assert_eq!(old.1,initial.input);
    let current=lineage(&f.controller,&next.session_id).await;assert!(current.2);assert_eq!(current.1,next.input);
    let retained=sessions(&f.controller).await;assert_eq!(retained.len(),2);
    let receipt=original(&f.controller,&id).await;
    let Outcome::Ok {data:Data::Operation(record)}=&receipt.outcome else {panic!("original assignment receipt")};
    assert_eq!(record.kind,"application_mutation");assert_eq!(record.state,"settled");
    assert_eq!(record.intended,serde_json::to_value(Operation::AssignTask {operation_id:id.clone(),task_id:initial.id.clone(),expected_revision:initial.revision,agent_id:d.id.clone()}).unwrap());
    assert_eq!(record.outcome,Some(serde_json::to_value(Outcome::Ok {data:Data::Task(Box::new(next.clone()))}).unwrap()));
    let replay=ui(&f.controller,"POST",&format!("{}/assign",route(&initial.id)),body.clone()).await;
    assert_eq!(replay.status,303);assert_eq!(task(&f.controller,&initial.id).await,next);assert_eq!(sessions(&f.controller).await,retained);
    let replay_receipt=original(&f.controller,&id).await;assert_eq!(replay_receipt.commit,receipt.commit);assert_eq!(replay_receipt.outcome,receipt.outcome);
    let conflict=ui(&f.controller,"POST",&format!("{}/assign",route(&initial.id)),operands(&rendered,&initial.assigned_agent_id)).await;
    assert_eq!(conflict.status,409);assert!(conflict.body.contains("operation_conflict"));
    let db=f.path.clone();drop(f);owner_closed(&db).await;let c=Controller::open_with_config(&db,config()).unwrap();
    assert_eq!(task(&c,&initial.id).await,next);assert_eq!(sessions(&c).await,retained);
    assert_eq!(ui(&c,"POST",&format!("{}/assign",route(&initial.id)),body).await.status,303);
    let reopened=original(&c,&id).await;assert_eq!(reopened.commit,receipt.commit);assert_eq!(reopened.outcome,receipt.outcome);
    let historical=ui(&c,"GET",&format!("/sessions/{}",initial.session_id),vec![]).await;
    assert!(historical.body.contains("Historical — authority closed"));assert!(!historical.body.contains("settings?task_id="));
    assert!(matches!(call(&c,Operation::ListTasks {after:None,limit:10}).await,Data::Tasks(v) if v.len()==1 && v[0]==next));
    drop(c);owner_closed(&db).await;
}

#[tokio::test]
async fn task_assignment_ui_refuses_missing_retired_stale_and_untrusted_form_operands() {
    Box::pin(assignment_refusals_journey()).await;
}
async fn assignment_refusals_journey() {
    let (f,initial,d)=seed("assignment-ui-negative-source").await;
    let route=format!("{}/assign",route(&initial.id));let retained=sessions(&f.controller).await;
    for (label,agent,revision,code) in [("missing","missing-agent",initial.revision,"not_found"),
        ("stale",d.id.as_str(),initial.revision+1,"stale_revision")] {
        let id=format!("assignment-ui-{label}");let body=form(&[("operation_id",id.clone()),("expected_revision",revision.to_string()),("agent_id",agent.into())]);
        let response=ui(&f.controller,"POST",&route,body).await;assert_eq!(response.status,409,"{}",response.body);assert!(response.body.contains(code));
        assert_eq!(task(&f.controller,&initial.id).await,initial);assert_eq!(sessions(&f.controller).await,retained);
        let r=original(&f.controller,&id).await;assert!(r.commit.is_some());
        let Outcome::Ok {data:Data::Operation(record)}=r.outcome else {panic!("committed refusal")};
        let saved:Outcome=serde_json::from_value(record.outcome.unwrap()).unwrap();assert!(matches!(saved,Outcome::Refused {code:c,..} if c==code));
    }
    call(&f.controller,Operation::RetireAgent {operation_id:"assignment-ui-retire".into(),agent_id:d.id.clone(),expected_revision:d.revision.clone()}).await;
    let body=form(&[("operation_id","assignment-ui-retired".into()),("expected_revision",initial.revision.to_string()),("agent_id",d.id)]);
    let refused=ui(&f.controller,"POST",&route,body).await;assert_eq!(refused.status,409);assert!(refused.body.contains("agent_retired"));
    let receipt=original(&f.controller,"assignment-ui-retired").await;assert!(receipt.commit.is_some());
    let Outcome::Ok {data:Data::Operation(record)}=receipt.outcome else {panic!("retired-target receipt")};
    let saved:Outcome=serde_json::from_value(record.outcome.unwrap()).unwrap();assert!(matches!(saved,Outcome::Refused {code,..} if code=="agent_retired"));
    let cross=form(&[("operation_id","assignment-ui-cross-origin".into()),("expected_revision",initial.revision.to_string()),("agent_id",initial.assigned_agent_id.clone())]);
    let response=Box::pin(handle(&f.controller,UiRequest {method:"POST".into(),path:route.clone(),body:cross,origin:Some("http://other.invalid".into()),expected_origin:ORIGIN.into()})).await;
    assert_eq!(response.status,403);
    let forged=form(&[("operation_id","assignment-ui-forged-caller".into()),("expected_revision",initial.revision.to_string()),("agent_id",initial.assigned_agent_id.clone()),("caller","worker".into())]);
    assert_eq!(ui(&f.controller,"POST",&route,forged).await.status,400);
    for id in ["assignment-ui-cross-origin","assignment-ui-forged-caller"] {
        let r=Box::pin(f.controller.handle(Request {protocol_revision:WIRE_REVISION,request_id:format!("lookup-{id}"),command:Operation::GetOperation {operation_id:id.into()}})).await;
        assert!(matches!(r.outcome,Outcome::Refused {code,..} if code=="not_found"));assert!(r.commit.is_none());
    }
    assert_eq!(task(&f.controller,&initial.id).await,initial);assert_eq!(sessions(&f.controller).await,retained);
    let db=f.path.clone();drop(f);owner_closed(&db).await;
}

#[tokio::test]
async fn task_assignment_ui_pending_and_terminal_views_cannot_offer_replacement_assignment() {
    Box::pin(pending_assignment_journey()).await;
    Box::pin(descendant_assignment_cancellation_journey()).await;
}
async fn pending_assignment_journey() {
    let (f,initial,d)=seed("assignment-ui-pending-source").await;
    let worker=Worker {id:"assignment-ui-controlled-worker".into(),incarnation:"1".into(),coding_capacity:1,native_capacity:0};
    call(&f.controller,Operation::RegisterWorker {operation_id:"assignment-ui-register-worker".into(),worker:worker.clone()}).await;
    let attempt=next_ui_attempt(&f.controller,&worker,&initial.id).await;
    let active=task(&f.controller,&initial.id).await;assert_eq!(active.attempt.as_ref().unwrap().id,attempt.id);
    let rendered=assignment_form(&ui(&f.controller,"GET",&route(&initial.id),vec![]).await,&initial.id);
    let id=value(&rendered,"operation_id");
    assert_eq!(ui(&f.controller,"POST",&format!("{}/assign",route(&initial.id)),operands(&rendered,&d.id)).await.status,303);
    let pending=task(&f.controller,&initial.id).await;let intent=pending.pending_assignment.as_ref().unwrap();
    assert_eq!(intent.operation_id,id);assert_eq!(pending.session_id,initial.session_id);assert_eq!(pending.input,initial.input);
    let receipt=original(&f.controller,&id).await;assert!(receipt.commit.is_some());
    let Outcome::Ok {data:Data::Operation(record)}=receipt.outcome else {panic!("pending-assignment receipt")};
    assert_eq!(record.outcome,Some(serde_json::to_value(Outcome::Ok {data:Data::Task(Box::new(pending.clone()))}).unwrap()));
    let page=ui(&f.controller,"GET",&route(&initial.id),vec![]).await;
    assert!(page.body.contains("Assignment pending"));assert!(page.body.contains(&format!("href=\"/operations/{id}\"")));
    assert!(page.body.contains(&d.id)&&page.body.contains(&d.revision));assert!(!page.body.contains(&format!("action=\"{}/assign\"",route(&initial.id))));
    assert!(!page.body.contains(&format!("href=\"/sessions/{}\"",intent.session.id)),"reserved session is not active authority");
    let cancel=form(&[("operation_id","assignment-ui-cancel-pending".into()),("expected_revision",pending.revision.to_string())]);
    assert_eq!(ui(&f.controller,"POST",&format!("{}/cancel",route(&initial.id)),cancel).await.status,303);
    let terminal=task(&f.controller,&initial.id).await;assert_eq!(terminal.status,TaskStatus::Cancelled);assert!(terminal.pending_assignment.is_none());
    assert_eq!(terminal.session_id, initial.session_id);assert_eq!(terminal.input, initial.input);
    assert_eq!(terminal.attempt.as_ref().unwrap().phase, AttemptPhase::StopRequested);
    assert!(!terminal.attempt.as_ref().unwrap().stopped, "cancellation is not termination evidence");
    assert_eq!(sessions(&f.controller).await.len(), 1);
    let page=ui(&f.controller,"GET",&route(&initial.id),vec![]).await;
    assert!(!page.body.contains(&format!("action=\"{}/assign\"",route(&initial.id))));assert!(page.body.contains("Request retry"));
    let body=form(&[("operation_id","assignment-ui-terminal".into()),("expected_revision",terminal.revision.to_string()),("agent_id",d.id)]);
    let rejected=ui(&f.controller,"POST",&format!("{}/assign",route(&initial.id)),body).await;
    assert_eq!(rejected.status,409);assert!(rejected.body.contains("invalid_transition"));
    assert_eq!(task(&f.controller,&initial.id).await,terminal);
    let db=f.path.clone();drop(f);owner_closed(&db).await;
}

async fn descendant_assignment_cancellation_journey() {
    // Real delegated lineage and assignment transactions; controlled commands
    // and stop observations only, with no worker/provider process.
    let f=Fixture::new().await;
    let deadline=now()+3_600_000;
    let flow=serde_json::to_string(&workflow(deadline)).unwrap();
    let submitted=ui(&f.controller,"POST","/tasks",native_submission("assignment-tree-parent",deadline,Some(&flow))).await;
    assert_eq!(submitted.status,303,"{}",submitted.body);
    let parent_id=task_id(&submitted).to_string();
    let worker=Worker {id:"assignment-tree-worker".into(),incarnation:"1".into(),coding_capacity:1,native_capacity:1};
    call(&f.controller,Operation::RegisterWorker {operation_id:"assignment-tree-register".into(),worker:worker.clone()}).await;
    let parent_attempt=next_ui_attempt(&f.controller,&worker,&parent_id).await;
    report(&f.controller,&parent_attempt,Observation::Usage {native_diagnostic_reserved:true,model_requests:1,input_tokens:None,output_tokens:None}).await;
    let parent=task(&f.controller,&parent_id).await;
    let child_input=borg_runtime::delegation::coding_child(&parent.input,"retain descendant work").unwrap();
    let Data::Task(child)=call(&f.controller,Operation::DelegateTask {
        operation_id:"assignment-tree-delegate".into(),parent_task_id:parent_id.clone(),
        parent_attempt_id:parent_attempt.id.clone(),generation:parent_attempt.generation,child:child_input,
    }).await else {panic!("actual delegated child")};
    assert_eq!(child.parent_task_id.as_deref(),Some(parent_id.as_str()));
    report(&f.controller,&parent_attempt,Observation::ProcessTreeStopped).await;
    let child_attempt=next_ui_attempt(&f.controller,&worker,&child.id).await;
    let child_before=task(&f.controller,&child.id).await;
    let d=target(&f.controller,&child_before,"assignment-tree-target").await;
    let rendered=assignment_form(&ui(&f.controller,"GET",&route(&child.id),vec![]).await,&child.id);
    let assignment_id=value(&rendered,"operation_id");
    assert_eq!(ui(&f.controller,"POST",&format!("{}/assign",route(&child.id)),operands(&rendered,&d.id)).await.status,303);
    let pending=task(&f.controller,&child.id).await;
    let reserved=pending.pending_assignment.as_ref().unwrap().session.clone();
    assert_eq!(pending.session_id,child_before.session_id);
    let assignment_receipt=original(&f.controller,&assignment_id).await;
    let parent_before=task(&f.controller,&parent_id).await;
    let sessions_before=sessions(&f.controller).await;assert_eq!(sessions_before.len(),2);
    // Stale root cancellation must not touch either visited-node projection.
    let stale=ui(&f.controller,"POST",&format!("{}/cancel",route(&parent_id)),form(&[
        ("operation_id","assignment-tree-stale-cancel".into()),("expected_revision",(parent_before.revision+1).to_string()),
    ])).await;
    assert_eq!(stale.status,409);assert!(stale.body.contains("stale_revision"));
    assert_eq!(task(&f.controller,&parent_id).await,parent_before);assert_eq!(task(&f.controller,&child.id).await,pending);
    let cancel_body=form(&[("operation_id","assignment-tree-cancel".into()),("expected_revision",parent_before.revision.to_string())]);
    assert_eq!(ui(&f.controller,"POST",&format!("{}/cancel",route(&parent_id)),cancel_body.clone()).await.status,303);
    let cancelled_parent=task(&f.controller,&parent_id).await;
    let cancelled_child=task(&f.controller,&child.id).await;
    assert_eq!(cancelled_parent.status,TaskStatus::Cancelled);assert_eq!(cancelled_child.status,TaskStatus::Cancelled);
    assert!(cancelled_parent.pending_assignment.is_none()&&cancelled_child.pending_assignment.is_none());
    assert_eq!(cancelled_child.session_id,child_before.session_id);assert_eq!(cancelled_child.input,child_before.input);
    assert_eq!(cancelled_child.attempt.as_ref().unwrap().id,child_attempt.id);
    assert_eq!(cancelled_child.attempt.as_ref().unwrap().phase,AttemptPhase::StopRequested);
    assert!(!cancelled_child.attempt.as_ref().unwrap().stopped);
    assert_eq!(sessions(&f.controller).await,sessions_before);
    let cancel_receipt=original(&f.controller,"assignment-tree-cancel").await;
    let db=f.path.clone();drop(f);owner_closed(&db).await;
    // Only inspect the closed owner. Verify both SQL intent and serialized Task,
    // so a superficially correct response cannot hide a pending durable intent.
    let store=rusqlite::Connection::open_with_flags(&db,rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let (state,to):(String,Option<String>)=store.query_row("SELECT state,to_session_id FROM task_assignments WHERE operation_id=?1",[&assignment_id],|r|Ok((r.get(0)?,r.get(1)?))).unwrap();
    assert_eq!(state,"cancelled");assert!(to.is_none());
    let raw:String=store.query_row("SELECT body FROM tasks WHERE id=?1",[&child.id],|r|r.get(0)).unwrap();
    let body:serde_json::Value=serde_json::from_str(&raw).unwrap();
    assert_eq!(body["task"],serde_json::to_value(&cancelled_child).unwrap());
    let reserved_count:i64=store.query_row("SELECT count(*) FROM sessions WHERE id=?1",[&reserved.id],|r|r.get(0)).unwrap();assert_eq!(reserved_count,0);
    let wait_state:String=store.query_row("SELECT state FROM waits WHERE parent=?1 AND child=?2",rusqlite::params![parent_id,child.id],|r|r.get(0)).unwrap();assert_eq!(wait_state,"settled");
    drop(store);
    let c=Controller::open_with_config(&db,config()).unwrap();
    assert_eq!(task(&c,&parent_id).await,cancelled_parent);assert_eq!(task(&c,&child.id).await,cancelled_child);
    assert_eq!(sessions(&c).await,sessions_before);
    let retained=original(&c,&assignment_id).await;assert_eq!(retained.commit,assignment_receipt.commit);assert_eq!(retained.outcome,assignment_receipt.outcome);
    assert_eq!(ui(&c,"POST",&format!("{}/cancel",route(&parent_id)),cancel_body).await.status,303);
    let replay=original(&c,"assignment-tree-cancel").await;assert_eq!(replay.commit,cancel_receipt.commit);assert_eq!(replay.outcome,cancel_receipt.outcome);
    // A later physical-stop observation cannot reactivate the cancelled intent.
    report(&c,&child_attempt,Observation::ProcessTreeStopped).await;
    let stopped=task(&c,&child.id).await;
    assert_eq!(stopped.status,TaskStatus::Cancelled);assert!(stopped.pending_assignment.is_none());
    assert!(stopped.attempt.as_ref().unwrap().stopped);assert_eq!(stopped.session_id,child_before.session_id);assert_eq!(stopped.input,child_before.input);
    assert_eq!(sessions(&c).await,sessions_before);
    let page=ui(&c,"GET",&route(&child.id),vec![]).await;
    assert!(!page.body.contains("Assignment pending"));assert!(!page.body.contains(&format!("action=\"{}/assign\"",route(&child.id))));
    assert!(page.body.contains("Request retry"));
    drop(c);owner_closed(&db).await;
}
