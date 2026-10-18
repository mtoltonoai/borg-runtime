//! Exact-target product reviews. None of these outcomes grant execution authority.
//! Called only inside the root's canonical operation/receipt transaction.
use super::database::{decode,encode,integer,refuse,valid_id,Fault,Result};
use super::collaboration_workflow::{self as workflow,Context,DomainChange,QueryContext};
use super::collaboration_documents::{self as documents,activity_text,new_record_id,query_page};
use crate::collaboration::{documents::*,workflow::*,questions::DecisionQueueItem};
use rusqlite::{params,OptionalExtension};

fn next(n:u64)->Result<u64> {
    n.checked_add(1).filter(|n|*n<=i64::MAX as u64).ok_or_else(||Fault::Refused("capacity","review revision exhausted".into()))
}
fn status(s:&ReviewStatus)->&'static str {match s{ReviewStatus::Open=>"open",ReviewStatus::Decided=>"decided",ReviewStatus::Withdrawn=>"withdrawn"}}
fn text(ctx:&Context<'_, '_>,value:&str,allow_empty:bool)->Result<()> {
    if (!allow_empty && value.trim().is_empty()) || value.contains('\0') {
        return refuse("invalid_input","review text is empty or invalid");
    }
    //= spec/impl/v2/implementation.md#limits-are-versioned-metadata
    //# An operation refused by a configured limit MUST identify the limiting metadata field.
    if value.len()>ctx.limits.text_bytes {return refuse("invalid_input","Configured limit exceeded: workflow.text_bytes")}
    Ok(())
}
fn bounded<T:serde::Serialize>(ctx:&Context<'_, '_>,value:&T)->Result<String> {
    let raw=encode(value)?;
    if raw.len()>ctx.limits.record_bytes{return refuse("capacity","Configured limit exceeded: workflow.record_bytes")}
    Ok(raw)
}
fn read<T:serde::Serialize+serde::de::DeserializeOwned>(q:&QueryContext<'_, '_>,raw:&str)->Result<T> {
    if raw.len()>q.limits.record_bytes{return Err(Fault::Store("stored review record exceeds its bound".into()))}
    decode(raw)
}

pub(super) fn target_record(target:&ReviewTarget)->RecordRef {
    match target {
        ReviewTarget::DocumentVersion{version}=>RecordRef::Document{document_id:version.document_id.clone()},
        ReviewTarget::AcceptedTaskResult{result}|ReviewTarget::AcceptedTaskCheckpoint{result}=>RecordRef::Task{task_id:result.task_id.clone()},
    }
}
fn validate_read_target(q:&QueryContext<'_, '_>,target:&ReviewTarget)->Result<()> {
    match target {
        ReviewTarget::DocumentVersion{version}=>{
            let saved=documents::get_version(q,&version.document_id,version.version)?;
            if saved.reference!=*version {return Err(Fault::Store("stored review document identity mismatch".into()))}
        }
        ReviewTarget::AcceptedTaskResult{result}|ReviewTarget::AcceptedTaskCheckpoint{result}=>{
            let scope=if matches!(target,ReviewTarget::AcceptedTaskCheckpoint{..}) {AcceptedReviewScope::Checkpoint}else{AcceptedReviewScope::Result};
            let canonical=workflow::accepted_target(q,&result.task_id,scope)?;
            if canonical!=*target {return Err(Fault::Store("stored review accepted result identity mismatch".into()))}
        }
    }
    Ok(())
}
pub(super) fn get_review(q:&QueryContext<'_, '_>,id:&str)->Result<Review> {
    if id.is_empty() || id.chars().any(char::is_control){return refuse("invalid_input","invalid review identity")}
    if id.len()>q.tx.limits.transport.identity_bytes{return refuse("invalid_input","Configured limit exceeded: transport.identity_bytes")}
    let row:Option<(i64,i64,String,String,String)>=q.tx.query_row(
        "SELECT revision,created_product_revision,CAST(substr(CAST(reviewer AS BLOB),1,?2) AS TEXT),CAST(substr(CAST(status AS BLOB),1,?2) AS TEXT),CAST(substr(CAST(body AS BLOB),1,?2) AS TEXT) FROM workflow_reviews WHERE id=?1",
        params![id,super::database::sql_bound(q.limits.record_bytes.saturating_add(1))?],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))).optional()?;
    let Some((revision,created,reviewer,state,body))=row else{return refuse("not_found","review not found")};
    let review:Review=read(q,&body)?;
    if review.id!=id || revision!=integer(review.revision)? || revision<=0
        || created!=integer(review.created_product_revision)? || created<=0 || state!=status(&review.status)
        || reviewer!=encode(&review.reviewer)? || review.changed_product_revision<review.created_product_revision
        || (review.status==ReviewStatus::Decided)!=review.decision_id.is_some() {
        return Err(Fault::Store("review index and canonical body disagree".into()));
    }
    if review.summary.trim().is_empty() || review.summary.len()>q.limits.text_bytes || review.summary.contains('\0') {return Err(Fault::Store("invalid stored review summary".into()))}
    workflow::validate_direct_principal(q.tx,&review.reviewer)?;
    workflow::validate_recorded_actor(q.tx,&review.requested_by)?;
    validate_read_target(q,&review.target)?;
    Ok(review)
}
pub(super) fn review_view(q:&QueryContext<'_, '_>,review:Review)->Result<ReviewView> {
    let decision:Option<(String,String)>=q.tx.query_row(
        "SELECT CAST(substr(CAST(id AS BLOB),1,?3) AS TEXT),CAST(substr(CAST(body AS BLOB),1,?2) AS TEXT) FROM workflow_review_decisions WHERE review_id=?1",
        params![review.id,super::database::sql_bound(q.limits.record_bytes.saturating_add(1))?,super::database::sql_bound(q.tx.limits.transport.identity_bytes.saturating_add(1))?],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
    let decision=decision.map(|(id,raw)|->Result<RecordedReviewDecision>{
        let d:RecordedReviewDecision=read(q,&raw)?;
        if d.id!=id || !valid_id(q.tx.limits,&d.id) || !valid_id(q.tx.limits,&d.operation_id) || d.explanation.len()>q.limits.text_bytes || d.explanation.contains('\0') || d.review_id!=review.id || d.target!=review.target || d.reviewer.principal!=review.reviewer
            || review.decision_id.as_ref()!=Some(&d.id) || d.product_revision>review.changed_product_revision || d.product_revision<=review.created_product_revision {
            return Err(Fault::Store("immutable review decision mismatch".into()));
        }
        workflow::validate_recorded_actor(q.tx,&d.reviewer)?;
        Ok(d)
    }).transpose()?;
    let withdrawal:Option<String>=q.tx.query_row(
        "SELECT CAST(substr(CAST(body AS BLOB),1,?2) AS TEXT) FROM workflow_review_withdrawals WHERE review_id=?1",
        params![review.id,super::database::sql_bound(q.limits.record_bytes.saturating_add(1))?],|r|r.get(0)).optional()?;
    let withdrawal=withdrawal.map(|raw|->Result<RecordedReviewWithdrawal>{
        let w:RecordedReviewWithdrawal=read(q,&raw)?;
        if !valid_id(q.tx.limits,&w.operation_id) || w.reason.trim().is_empty() || w.reason.len()>q.limits.text_bytes || w.reason.contains('\0') || w.review_id!=review.id || w.target!=review.target || w.product_revision>review.changed_product_revision || w.product_revision<=review.created_product_revision {
            return Err(Fault::Store("immutable review withdrawal mismatch".into()));
        }
        workflow::validate_recorded_actor(q.tx,&w.actor)?;
        Ok(w)
    }).transpose()?;
    let consistent=match review.status {
        ReviewStatus::Open=>decision.is_none()&&withdrawal.is_none(),
        ReviewStatus::Decided=>decision.is_some()&&withdrawal.is_none(),
        ReviewStatus::Withdrawn=>withdrawal.is_some()&&decision.is_none(),
    };
    if !consistent{return Err(Fault::Store("review state and outcome record disagree".into()))}
    let current_responsible_owner=workflow::record_owner_revision(q.tx,&target_record(&review.target))?.owner;
    Ok(ReviewView{review,decision,withdrawal,current_responsible_owner})
}
fn save(ctx:&Context<'_, '_>,before:u64,review:&Review)->Result<()> {
    if ctx.tx.execute("UPDATE workflow_reviews SET revision=?3,status=?4,body=?5 WHERE id=?1 AND revision=?2",
        params![review.id,integer(before)?,integer(review.revision)?,status(&review.status),bounded(ctx,review)?])?!=1 {
        return refuse("revision_conflict","review revision changed");
    }
    Ok(())
}
fn open(q:&QueryContext<'_, '_>,id:&str,expected:u64)->Result<ReviewView> {
    let view=review_view(q,get_review(q,id)?)?;
    //= spec/impl/v2/implementation.md#collaboration-records-have-stable-identity-and-provenance
    //# A mutation of an existing collaboration record MUST compare the expected record revision.
    if view.review.revision!=expected{return refuse("revision_conflict","review revision changed")}
    if view.review.status!=ReviewStatus::Open{return refuse("review_closed","review already has a terminal product outcome")}
    Ok(view)
}

pub(super) fn apply_review(ctx:&Context<'_, '_>,request:&ReviewMutation)->Result<DomainChange<ReviewMutationOutcome>> {
    let q=ctx.query();
    match request {
        ReviewMutation::Request{target,reviewer,summary}=>{
            text(ctx,summary,false)?;
            workflow::validate_new_principal(q.tx,reviewer)?;
            //= spec/impl/v2/implementation.md#reviews-bind-the-exact-subject-version
            //# A review MUST bind the exact immutable subject version or result reference it evaluates.
            workflow::validate_review_target(ctx,target)?;
            workflow::ensure_capacity(ctx,1,0)?;
            let id=new_record_id(ctx,"review")?;
            let review=Review{id:id.clone(),revision:1,target:target.clone(),reviewer:reviewer.clone(),summary:summary.clone(),
                requested_by:ctx.actor.clone(),created_product_revision:ctx.product_revision,changed_product_revision:ctx.product_revision,
                status:ReviewStatus::Open,decision_id:None};
            ctx.tx.execute("INSERT INTO workflow_reviews(id,revision,created_product_revision,reviewer,status,body) VALUES(?1,1,?2,?3,'open',?4)",
                params![id,integer(ctx.product_revision)?,encode(reviewer)?,bounded(ctx,&review)?])?;
            let effect=WorkflowEffect::Activity{subject:RecordRef::Review{review_id:id},subject_revision:1,
                detail:ActivityDetail::Review(ReviewChange::Requested{target:target.clone(),reviewer:reviewer.clone(),summary:activity_text(ctx,summary)})};
            Ok(DomainChange{outcome:ReviewMutationOutcome::Review(review_view(&q,review)?),effects:vec![effect]})
        }
        ReviewMutation::Decide{review_id,expected_revision,verdict,explanation}=>{
            text(ctx,explanation,true)?;
            let mut view=open(&q,review_id,*expected_revision)?;
            //= spec/impl/v2/implementation.md#reviews-bind-the-exact-subject-version
            //# A review decision MUST match its recorded direct reviewer to the caller context established by the controller.
            if ctx.actor.principal!=view.review.reviewer{return refuse("wrong_reviewer","caller is not the recorded direct reviewer")}
            workflow::ensure_capacity(ctx,0,1)?;
            let decision_id=new_record_id(ctx,"review-decision")?;
            //= spec/impl/v2/implementation.md#reviews-bind-the-exact-subject-version
            //# A review decision MUST retain the reviewed subject identity.
            let decision=RecordedReviewDecision{id:decision_id.clone(),review_id:review_id.clone(),target:view.review.target.clone(),verdict:verdict.clone(),
                explanation:explanation.clone(),reviewer:ctx.actor.clone(),product_revision:ctx.product_revision,operation_id:ctx.operation_id.into()};
            //= spec/impl/v2/implementation.md#reviews-bind-the-exact-subject-version
            //# A recorded review decision MUST be immutable.
            ctx.tx.execute("INSERT INTO workflow_review_decisions(id,review_id,body) VALUES(?1,?2,?3)",params![decision_id,review_id,bounded(ctx,&decision)?])?;
            view.review.status=ReviewStatus::Decided;
            view.review.decision_id=Some(decision_id.clone());
            view.review.revision=next(view.review.revision)?;
            view.review.changed_product_revision=ctx.product_revision;
            save(ctx,*expected_revision,&view.review)?;
            let resolution=ReviewResolution::Decided{review_id:review_id.clone(),decision_id,target:view.review.target.clone(),verdict:verdict.clone()};
            let effects=effects(&view.review,resolution);
            Ok(DomainChange{outcome:ReviewMutationOutcome::Review(review_view(&q,view.review)?),effects})
        }
        ReviewMutation::Withdraw{review_id,expected_revision,reason}=>{
            text(ctx,reason,false)?;
            let mut view=open(&q,review_id,*expected_revision)?;
            if ctx.actor.principal!=view.review.requested_by.principal && ctx.actor.principal!=view.current_responsible_owner
                && ctx.actor.principal!=PrincipalRef::TrustedHostOperator {
                return refuse("not_review_owner","caller is not the requester, current responsible owner or trusted-host operator");
            }
            workflow::ensure_capacity(ctx,0,1)?;
            //= spec/impl/v2/implementation.md#reviews-bind-the-exact-subject-version
            //# Withdrawing a review MUST record its acting caller context and reason.
            let withdrawal=RecordedReviewWithdrawal{review_id:review_id.clone(),target:view.review.target.clone(),reason:reason.clone(),
                actor:ctx.actor.clone(),product_revision:ctx.product_revision,operation_id:ctx.operation_id.into()};
            ctx.tx.execute("INSERT INTO workflow_review_withdrawals(review_id,body) VALUES(?1,?2)",params![review_id,bounded(ctx,&withdrawal)?])?;
            view.review.status=ReviewStatus::Withdrawn;
            view.review.revision=next(view.review.revision)?;
            view.review.changed_product_revision=ctx.product_revision;
            save(ctx,*expected_revision,&view.review)?;
            let resolution=ReviewResolution::Withdrawn{review_id:review_id.clone(),target:view.review.target.clone(),action_revision:view.review.revision,reason:reason.clone()};
            //= spec/impl/v2/implementation.md#reviews-bind-the-exact-subject-version
            //# A review withdrawal MUST publish committed activity identifying the affected review.
            let effects=effects(&view.review,resolution);
            Ok(DomainChange{outcome:ReviewMutationOutcome::Review(review_view(&q,view.review)?),effects})
        }
    }
}
fn effects(review:&Review,resolution:ReviewResolution)->Vec<WorkflowEffect> {
    let subject=RecordRef::Review{review_id:review.id.clone()};
    // Root materializes both intents with the record mutation, its receipt and all
    // resolved waits in one SQLite transaction. This module never commits itself.
    vec![WorkflowEffect::Activity{subject:subject.clone(),subject_revision:review.revision,detail:ActivityDetail::Review(ReviewChange::Closed{resolution:resolution.clone()})},
        WorkflowEffect::Resolve{resolution:ResolverOutcome::Review(resolution),requested_by:review.requested_by.clone(),linked_record:subject}]
}

pub(super) fn query_review(q:&QueryContext<'_, '_>,query:&ReviewQuery)->Result<ReviewQueryOutcome> {
    match query {
        ReviewQuery::Get{review_id}=>Ok(ReviewQueryOutcome::Review(review_view(q,get_review(q,review_id)?)?)),
        ReviewQuery::AcceptedTarget{task_id,scope}=>Ok(ReviewQueryOutcome::AcceptedTarget(workflow::accepted_target(q,task_id,scope.clone())?)),
        ReviewQuery::List{record,status:filter,after,limit}=>{
            if let Some(record)=record {let _=workflow::record_owner_revision(q.tx,record)?;}
            let scope=encode(&(record,filter))?;
            let page=query_page(q,"reviews",&scope,after,*limit,|start,bound|{
                let mut stmt=q.tx.prepare("SELECT id FROM workflow_reviews WHERE id>?1 ORDER BY id LIMIT ?2")?;
                let ids=stmt.query_map(params![start,bound],|r|r.get::<_,String>(0))?.collect::<std::result::Result<Vec<_>,_>>()?;
                ids.into_iter().map(|id|{
                    let r=get_review(q,&id)?;
                    let matched=filter.as_ref().is_none_or(|s|*s==r.status) && record.as_ref().is_none_or(|v|*v==target_record(&r.target)||*v==RecordRef::Review{review_id:r.id.clone()});
                    Ok((id,matched.then_some(r)))
                }).collect()
            })?;
            Ok(ReviewQueryOutcome::Reviews(page))
        }
    }
}

/// Canonical queue rows; D merges these with questions without a copied queue.
/// Tuple/through validation and complete byte-bounded continuation belong to D.
pub(super) fn pending_review_rows(q:&QueryContext<'_, '_>,viewer:&PrincipalRef,after:Option<(u64,String)>,through:u64,limit:usize)->Result<Vec<DecisionQueueItem>> {
    workflow::validate_direct_principal(q.tx,viewer)?;
    if limit==0{return refuse("invalid_input","review queue row bound must be positive")}
    if limit>q.limits.page_items.saturating_add(1){return refuse("invalid_input","Configured limit exceeded: workflow.page_items")}
    let (revision,id)=after.unwrap_or((0,String::new()));
    let mut stmt=q.tx.prepare("SELECT id FROM workflow_reviews WHERE reviewer=?1 AND status='open' AND created_product_revision<=?2 AND (created_product_revision>?3 OR (created_product_revision=?3 AND id>?4)) ORDER BY created_product_revision,id LIMIT ?5")?;
    let ids=stmt.query_map(params![encode(viewer)?,integer(through)?,integer(revision)?,id,super::database::sql_bound(limit)?],|r|r.get::<_,String>(0))?.collect::<std::result::Result<Vec<_>,_>>()?;
    ids.into_iter().map(|id|{
        let view=review_view(q,get_review(q,&id)?)?;
        let review=view.review;
        if review.status!=ReviewStatus::Open || review.reviewer!=*viewer{return Err(Fault::Store("review queue index mismatch".into()))}
        Ok(DecisionQueueItem::Review{review_id:review.id,revision:review.revision,created_product_revision:review.created_product_revision,
            recipient:review.reviewer,target:review.target,summary:review.summary})
    }).collect()
}


/// Root's cross-domain comment helper preserves the immutable subject/outcome.
pub(super) fn touch(ctx:&Context<'_, '_>,review_id:&str,expected_revision:u64)->Result<u64> {
    let q=ctx.query();
    let mut view=review_view(&q,get_review(&q,review_id)?)?;
    if view.review.revision!=expected_revision{return refuse("revision_conflict","review revision changed")}
    view.review.revision=next(view.review.revision)?;
    view.review.changed_product_revision=ctx.product_revision;
    save(ctx,expected_revision,&view.review)?;
    Ok(view.review.revision)
}

/// Read-only root startup seam. Child tables are scanned independently so an
/// orphan outcome is refused even if a damaged writer bypassed foreign keys.
// Traceability scope: Selected retained review state associated with task workflow;
// this does not replace root's other stored-state and schema validators.
//= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
//# Corrupt stored task state MUST be refused without rewriting its data.
pub(super) fn validate_stored(q:&QueryContext<'_, '_>)->Result<()> {
    let committed=workflow::product_revision(q.tx)?;
    let id_bytes=q.tx.limits.transport.identity_bytes.saturating_add(1);
    let mut heads=q.tx.prepare("SELECT CAST(substr(CAST(id AS BLOB),1,?1) AS TEXT) FROM workflow_reviews ORDER BY id LIMIT ?2")?;
    let mut rows=heads.query(params![super::database::sql_bound(id_bytes)?,documents::startup_scan_limit(q.limits.records)?])?;
    let mut count=0usize;
    while let Some(row)=rows.next()? {
        count+=1;if count>q.limits.records{return Err(Fault::Store("stored review count exceeds workflow.records".into()))}
        let id:String=row.get(0)?;let view=review_view(q,get_review(q,&id)?)?;
        if view.review.changed_product_revision>committed {return Err(Fault::Store("review exceeds committed product history".into()))}
    }
    let mut decisions=q.tx.prepare("SELECT CAST(substr(CAST(id AS BLOB),1,?1) AS TEXT),CAST(substr(CAST(review_id AS BLOB),1,?1) AS TEXT) FROM workflow_review_decisions ORDER BY id LIMIT ?2")?;
    let mut rows=decisions.query(params![super::database::sql_bound(id_bytes)?,documents::startup_scan_limit(q.limits.revisions)?])?;
    let mut count=0usize;
    while let Some(row)=rows.next()? {
        count+=1;if count>q.limits.revisions{return Err(Fault::Store("stored decision count exceeds workflow.revisions".into()))}
        let id:String=row.get(0)?;let review_id:String=row.get(1)?;
        if !valid_id(q.tx.limits,&id) || !valid_id(q.tx.limits,&review_id){return Err(Fault::Store("invalid retained decision identity".into()))}
        let view=review_view(q,get_review(q,&review_id)?)?;
        if view.decision.as_ref().is_none_or(|decision|decision.id!=id || decision.product_revision>committed) {
            return Err(Fault::Store("retained decision association mismatch".into()));
        }
    }
    let mut withdrawals=q.tx.prepare("SELECT CAST(substr(CAST(review_id AS BLOB),1,?1) AS TEXT) FROM workflow_review_withdrawals ORDER BY review_id LIMIT ?2")?;
    let mut rows=withdrawals.query(params![super::database::sql_bound(id_bytes)?,documents::startup_scan_limit(q.limits.revisions)?])?;
    let mut count=0usize;
    while let Some(row)=rows.next()? {
        count+=1;if count>q.limits.revisions{return Err(Fault::Store("stored withdrawal count exceeds workflow.revisions".into()))}
        let review_id:String=row.get(0)?;
        if !valid_id(q.tx.limits,&review_id){return Err(Fault::Store("invalid retained withdrawal identity".into()))}
        let view=review_view(q,get_review(q,&review_id)?)?;
        if view.withdrawal.as_ref().is_none_or(|withdrawal|withdrawal.review_id!=review_id || withdrawal.product_revision>committed) {
            return Err(Fault::Store("retained withdrawal association mismatch".into()));
        }
    }
    Ok(())
}
