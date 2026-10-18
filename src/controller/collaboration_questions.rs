//! Questions and append-only decisions in the controller's existing transaction.
//! The caller owns receipt storage, product revision, resolver settlement and COMMIT.
use super::{bounded_json_size, database::*};
use super::collaboration_workflow::{Context, QueryContext, DomainChange, record_owner, ensure_capacity, product_revision, validate_recorded_actor};
use crate::collaboration::{Page, questions::*, workflow::*};
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::collections::BTreeSet;

fn id(tx: &StoreContext<'_>, value: &str) -> Result<()> {
    if !valid_id(tx.limits, value) { return refuse("invalid_input", "invalid collaboration identity: transport.identity_bytes"); }
    Ok(())
}
fn text(value: &str, maximum: usize, required: bool) -> Result<()> {
    if value.len() > maximum { return refuse("input_bound", "question text exceeds workflow.text_bytes"); }
    if value.contains('\0') || (required && value.trim().is_empty()) {
        return refuse("invalid_input", "question text must be nonempty and contain no NUL");
    }
    Ok(())
}
fn bounded<T: Serialize>(value: &T, maximum: usize) -> Result<()> {
    bounded_json_size(value, maximum)
        .map(|_| ())
        .map_err(|_| Fault::Refused("record_bound", "question record exceeds workflow.record_bytes".into()))
}
fn stored<T: DeserializeOwned + Serialize>(raw: &str, maximum: usize) -> Result<T> {
    if raw.len() > maximum { return Err(Fault::Store("stored question record exceeds configured bound".into())); }
    decode(raw)
}
fn record_key(record: &RecordRef) -> (&'static str, &str) {
    match record {
        RecordRef::Task { task_id } => ("task", task_id),
        RecordRef::Question { question_id } => ("question", question_id),
        RecordRef::Document { document_id } => ("document", document_id),
        RecordRef::Review { review_id } => ("review", review_id),
    }
}
fn status(value: &QuestionStatus) -> &'static str {
    match value { QuestionStatus::Open => "open", QuestionStatus::Answered => "answered", QuestionStatus::Cancelled => "cancelled", QuestionStatus::Superseded => "superseded", QuestionStatus::Moot => "moot" }
}
fn recipient(tx: &StoreContext<'_>, value: &PrincipalRef) -> Result<()> {
    match value {
        PrincipalRef::TrustedHostOperator => Ok(()),
        PrincipalRef::Agent { agent_id } => {
            id(tx, agent_id)?;
            let found: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM agents WHERE id=?1)", [agent_id], |r| r.get(0))?;
            if !found { return refuse("not_found", "question recipient agent does not exist"); }
            Ok(())
        }
    }
}
fn validate_pose(ctx: &Context<'_, '_>, value: &PoseQuestion) -> Result<()> {
    id(ctx.tx, record_key(&value.record).1)?;
    record_owner(ctx.tx, &value.record)?;
    super::collaboration_workflow::validate_new_principal(ctx.tx, &value.recipient)?;
    text(&value.prompt, ctx.limits.text_bytes, true)?;
    if value.options.is_empty() || value.options.len() > ctx.limits.options {
        return refuse("input_bound", "question option count exceeds workflow.options or is empty");
    }
    let mut keys = BTreeSet::new();
    for option in &value.options {
        id(ctx.tx, &option.id)?;
        text(&option.label, ctx.limits.text_bytes, true)?;
        text(&option.explanation, ctx.limits.text_bytes, false)?;
        if !keys.insert(&option.id) { return refuse("invalid_input", "question option IDs must be unique"); }
    }
    if !keys.contains(&value.recommended_option_id) {
        return refuse("invalid_input", "question recommendation must name one offered option");
    }
    bounded(value, ctx.limits.record_bytes)
}
/// The posted prompt contains the selected recommendation itself. This exact
/// line is composed once before acceptance; no natural-language inference is used.
fn recommendation_suffix(value: &PoseQuestion) -> Result<String> {
    let option=value.options.iter().find(|option|option.id==value.recommended_option_id)
        .ok_or_else(||Fault::Refused("invalid_input","question recommendation must name one offered option".into()))?;
    Ok(format!("\n\nRecommended option: {} — {}.",encode(&option.id)?,encode(&option.label)?))
}
//= spec/impl/v2/implementation.md#questions-preserve-their-posed-content-and-outcomes
//# A question's prompt MUST state the recommended option.
fn canonical_pose(ctx:&Context<'_, '_>,value:&PoseQuestion)->Result<PoseQuestion> {
    validate_pose(ctx,value)?;
    let suffix=recommendation_suffix(value)?;
    let mut posed=value.clone();
    if !posed.prompt.ends_with(&suffix) {
        if posed.prompt.contains("\n\nRecommended option: ") {
            return refuse("invalid_input","question prompt has a different reserved recommendation line");
        }
        posed.prompt.push_str(&suffix);
    }
    text(&posed.prompt,ctx.limits.text_bytes,true)?;
    bounded(&posed,ctx.limits.record_bytes)?;
    Ok(posed)
}
//= spec/impl/v2/implementation.md#questions-preserve-their-posed-content-and-outcomes
//# A question answer MUST match the direct recipient recorded on the question to the caller context established by the controller.
fn check_answer(ctx: &Context<'_, '_>, question: &Question, value: &QuestionAnswer) -> Result<()> {
    if question.posed.recipient != ctx.actor.principal {
        return refuse("recipient_mismatch", "only the question's direct recipient may answer");
    }
    if !question.posed.options.iter().any(|option| option.id == value.option_id) {
        return refuse("invalid_input", "answer must select an offered option");
    }
    text(&value.explanation, ctx.limits.text_bytes, false)?;
    bounded(value, ctx.limits.record_bytes)
}
//= spec/impl/v2/implementation.md#questions-preserve-their-posed-content-and-outcomes
//# An answer replacement or withdrawal MUST be accepted only from the question's recorded direct recipient.
fn current_recipient(ctx: &Context<'_, '_>, question: &Question) -> Result<()> {
    if question.posed.recipient != ctx.actor.principal {
        return refuse("recipient_mismatch", "only the question's direct recipient may change its answer");
    }
    Ok(())
}
//= spec/impl/v2/implementation.md#questions-preserve-their-posed-content-and-outcomes
//# An answered question MUST NOT be cancelled or superseded.
fn open(question: &Question) -> Result<()> {
    if question.status != QuestionStatus::Open { return refuse("resolver_closed", "question has already resolved"); }
    Ok(())
}
fn answered(question: &Question, expected: &str, effective: bool) -> Result<()> {
    if question.status != QuestionStatus::Answered { return refuse("invalid_transition", "question has no replaceable answer"); }
    if question.latest_answer_id.as_deref() != Some(expected)
        || (effective && question.active_answer_id.as_deref() != Some(expected)) {
        return refuse("stale_answer", "answer head or effective answer differs");
    }
    Ok(())
}

pub(super) fn get(tx: &StoreContext<'_>, question_id: &str, maximum: usize) -> Result<Question> {
    id(tx, question_id)?;
    let row: Option<(i64,i64,String,String,String,String,String)> = tx.query_row(
        "SELECT revision,created_product_revision,CAST(substr(CAST(recipient AS BLOB),1,?2) AS TEXT),CAST(substr(CAST(status AS BLOB),1,?2) AS TEXT),CAST(substr(CAST(record_kind AS BLOB),1,?2) AS TEXT),CAST(substr(CAST(record_id AS BLOB),1,?2) AS TEXT),CAST(substr(CAST(body AS BLOB),1,?2) AS TEXT) FROM workflow_questions WHERE id=?1",
        params![question_id, super::database::sql_bound(maximum.saturating_add(1))?],
        |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?)),
    ).optional()?;
    let Some((revision,created,recipient_json,stored_status,kind,record_id,body))=row else {
        return refuse("not_found", "question does not exist");
    };
    // Each index string is part of the canonical bounded question. Project a
    // byte prefix (not SQLite TEXT substr, which stops at NUL), then refuse an
    // over-bound index before decoding or following anything from the body.
    //= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
    //# Corrupt stored task state MUST be refused without rewriting its data.
    if [recipient_json.as_str(),stored_status.as_str(),kind.as_str(),record_id.as_str()]
        .iter().any(|value|value.len()>maximum) {
        return Err(Fault::Store("stored question index exceeds configured workflow.record_bytes".into()));
    }
    let q: Question = stored(&body, maximum)?;
    validate_recorded_actor(tx, &q.poser)?;
    if q.id != question_id || q.revision == 0 || q.revision != unsigned(revision)?
        || q.created_product_revision == 0 || q.created_product_revision != unsigned(created)?
        || q.changed_product_revision < q.created_product_revision
        || encode(&q.posed.recipient)? != recipient_json || status(&q.status) != stored_status
        || record_key(&q.posed.record) != (kind.as_str(), record_id.as_str()) {
        return Err(Fault::Store("question/index identity mismatch".into()));
    }
    let valid = match q.status {
        QuestionStatus::Open => q.latest_answer_id.is_none() && q.active_answer_id.is_none() && q.superseded_by.is_none(),
        QuestionStatus::Answered => q.latest_answer_id.is_some() && q.superseded_by.is_none()
            && (q.active_answer_id.is_none() || q.active_answer_id == q.latest_answer_id),
        QuestionStatus::Moot => q.latest_answer_id.is_some() && q.active_answer_id.is_none() && q.superseded_by.is_none(),
        QuestionStatus::Cancelled => q.latest_answer_id.is_none() && q.active_answer_id.is_none() && q.superseded_by.is_none(),
        QuestionStatus::Superseded => q.latest_answer_id.is_none() && q.active_answer_id.is_none() && q.superseded_by.is_some(),
    };
    if !valid { return Err(Fault::Store("question lifecycle fields disagree".into())); }
    if let Some(answer_id)=&q.latest_answer_id {
        let answer=get_answer(tx, &AnswerRef {question_id:q.id.clone(),answer_id:answer_id.clone()}, maximum)?;
        if answer.author.principal != q.posed.recipient
            || !q.posed.options.iter().any(|option| option.id == answer.answer.option_id)
            || answer.product_revision > q.changed_product_revision {
            return Err(Fault::Store("question answer head disagrees".into()));
        }
    }
    Ok(q)
}
pub(super) fn get_answer(tx:&StoreContext<'_>, reference:&AnswerRef, maximum:usize)->Result<RecordedAnswer> {
    id(tx,&reference.question_id)?;id(tx,&reference.answer_id)?;
    let row:Option<(String,i64,String)>=tx.query_row(
        "SELECT question_id,product_revision,CAST(substr(CAST(body AS BLOB),1,?2) AS TEXT) FROM workflow_answers WHERE id=?1",
        params![reference.answer_id,super::database::sql_bound(maximum.saturating_add(1))?],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?)),
    ).optional()?;
    let Some((question_id,revision,raw))=row else{return refuse("not_found","answer does not exist")};
    let answer:RecordedAnswer=stored(&raw,maximum)?;
    validate_recorded_actor(tx, &answer.author)?;
    if question_id!=reference.question_id || answer.id!=reference.answer_id || answer.question_id!=question_id
        || answer.product_revision!=unsigned(revision)? || answer.product_revision==0 {
        return Err(Fault::Store("answer/index identity mismatch".into()));
    }
    Ok(answer)
}
pub(super) fn view(tx:&StoreContext<'_>, question:Question, maximum:usize)->Result<QuestionView> {
    let current_responsible_owner=record_owner(tx,&question.posed.record)?;
    let active_answer=question.active_answer_id.as_ref().map(|answer_id|get_answer(tx,&AnswerRef{question_id:question.id.clone(),answer_id:answer_id.clone()},maximum)).transpose()?;
    Ok(QuestionView {question,active_answer,current_responsible_owner})
}

// Traceability scope: Question identities come from a transactional durable counter.
//= spec/impl/v2/implementation.md#collaboration-records-have-stable-identity-and-provenance
//# Every collaboration record MUST have a durable identity.

fn next_id(ctx:&Context<'_, '_>,kind:&str,prefix:&str)->Result<String> {
    let value=format!("{prefix}-{}",next(ctx.tx,kind)?);id(ctx.tx,&value)?;Ok(value)
}
fn insert_question(ctx:&Context<'_, '_>,posed:&PoseQuestion,supersedes:Option<String>)->Result<Question> {
    let posed=canonical_pose(ctx,posed)?;
    ensure_capacity(ctx,1,0)?;
    //= spec/impl/v2/implementation.md#questions-preserve-their-posed-content-and-outcomes
    //# A blocking question MUST take no default answer.
    //= spec/impl/v2/implementation.md#questions-preserve-their-posed-content-and-outcomes
    //# A replacement question MUST record the acting poser and the question it supersedes.
    let q=Question{id:next_id(ctx,"workflow_question","question")?,revision:1,
        created_product_revision:ctx.product_revision,changed_product_revision:ctx.product_revision,
        posed:posed.clone(),poser:ctx.actor.clone(),status:QuestionStatus::Open,
        latest_answer_id:None,active_answer_id:None,superseded_by:None,supersedes};
    bounded(&q,ctx.limits.record_bytes)?;
    let(kind,record_id)=record_key(&q.posed.record);
    ctx.tx.execute("INSERT INTO workflow_questions(id,revision,created_product_revision,recipient,status,record_kind,record_id,body) VALUES(?1,1,?2,?3,'open',?4,?5,?6)",
        params![q.id,integer(q.created_product_revision)?,encode(&q.posed.recipient)?,kind,record_id,encode(&q)?])?;
    Ok(q)
}
fn write_head(ctx:&Context<'_, '_>,before:&Question,after:&Question)->Result<()> {
    //= spec/impl/v2/implementation.md#questions-preserve-their-posed-content-and-outcomes
    //# A posted question's prompt, options and typed decision fields MUST be immutable.
    //= spec/impl/v2/implementation.md#questions-preserve-their-posed-content-and-outcomes
    //# A question cancellation or supersession MUST preserve the original poser.
    if before.posed!=after.posed || before.poser!=after.poser || before.id!=after.id
        || before.created_product_revision!=after.created_product_revision || before.supersedes!=after.supersedes {
        return Err(Fault::Store("question immutable fields changed".into()));
    }
    bounded(after,ctx.limits.record_bytes)?;
    if ctx.tx.execute("UPDATE workflow_questions SET revision=?3,status=?4,body=?5 WHERE id=?1 AND revision=?2",
        params![before.id,integer(before.revision)?,integer(after.revision)?,status(&after.status),encode(after)?])? != 1 {
        return Err(Fault::Store("question CAS changed inside owned transaction".into()));
    }
    Ok(())
}
fn insert_answer(ctx:&Context<'_, '_>,question:&Question,value:&QuestionAnswer,replaces:Option<String>)->Result<RecordedAnswer> {
    check_answer(ctx,question,value)?;
    ensure_capacity(ctx,0,1)?;
    let answer=RecordedAnswer{id:next_id(ctx,"workflow_answer","answer")?,question_id:question.id.clone(),
        answer:value.clone(),author:ctx.actor.clone(),replaces,product_revision:ctx.product_revision,
        operation_id:ctx.operation_id.into()};
    bounded(&answer,ctx.limits.record_bytes)?;
    //= spec/impl/v2/implementation.md#questions-preserve-their-posed-content-and-outcomes
    //# A recorded answer MUST be immutable.
    //= spec/impl/v2/implementation.md#questions-preserve-their-posed-content-and-outcomes
    //# An answer replacement or withdrawal MUST retain the original answer record.
    ctx.tx.execute("INSERT INTO workflow_answers(id,question_id,product_revision,body) VALUES(?1,?2,?3,?4)",
        params![answer.id,answer.question_id,integer(answer.product_revision)?,encode(&answer)?])?;
    Ok(answer)
}
fn insert_action(ctx:&Context<'_, '_>,question:&Question,action:QuestionAction)->Result<RecordedQuestionAction> {
    ensure_capacity(ctx,0,1)?;
    //= spec/impl/v2/implementation.md#questions-preserve-their-posed-content-and-outcomes
    //# A moot mark MUST record its acting caller context and the answer that made the decision moot.
    //= spec/impl/v2/implementation.md#questions-preserve-their-posed-content-and-outcomes
    //# A question cancellation or supersession MUST record its acting caller context and reason.
    let value=RecordedQuestionAction{question_id:question.id.clone(),question_revision:question.revision,
        product_revision:ctx.product_revision,actor:ctx.actor.clone(),operation_id:ctx.operation_id.into(),action};
    bounded(&value,ctx.limits.record_bytes)?;
    ctx.tx.execute("INSERT INTO workflow_question_actions(question_id,question_revision,product_revision,body) VALUES(?1,?2,?3,?4)",
        params![value.question_id,integer(value.question_revision)?,integer(value.product_revision)?,encode(&value)?])?;
    Ok(value)
}

/// SQL and transition portion. The common wrapper consumes the exact returned
/// transition to stage activity/resolution/delivery before its existing COMMIT.
fn transition(ctx:&Context<'_, '_>,mutation:&QuestionMutation)->Result<QuestionMutationOutcome> {
    if let QuestionMutation::Pose{question}=mutation {
        let q=insert_question(ctx,question,None)?;
        return Ok(QuestionMutationOutcome{question:view(ctx.tx,q,ctx.limits.record_bytes)?,new_answer:None,replacement:None,action:None});
    }
    let (question_id,expected)=mutation.expected_record().ok_or_else(||Fault::Store("question mutation missing head".into()))?;
    let before=get(ctx.tx,question_id,ctx.limits.record_bytes)?;
    if before.revision != expected {return refuse("stale_revision","question revision differs")}
    let mut q=before.clone();
    q.revision=q.revision.checked_add(1).ok_or_else(||Fault::Store("question revision exhausted".into()))?;
    integer(q.revision)?;
    q.changed_product_revision=ctx.product_revision;
    let(mut new_answer,mut replacement,mut action)=(None,None,None);
    match mutation {
        QuestionMutation::Answer{answer,..} => {
            open(&q)?;
            let value=insert_answer(ctx,&q,answer,None)?;
            q.status=QuestionStatus::Answered;q.latest_answer_id=Some(value.id.clone());q.active_answer_id=Some(value.id.clone());new_answer=Some(value);
        }
        //= spec/impl/v2/implementation.md#questions-preserve-their-posed-content-and-outcomes
        //# An open question MUST remain cancellable or supersedable by its current responsible record owner when the asker is inactive.
        QuestionMutation::Cancel{reason,..} => {
            open(&q)?;text(reason,ctx.limits.text_bytes,true)?;q.status=QuestionStatus::Cancelled;
            action=Some(insert_action(ctx,&q,QuestionAction::Cancelled{reason:reason.clone()})?);
        }
        //= spec/impl/v2/implementation.md#questions-preserve-their-posed-content-and-outcomes
        //# A question cancellation or supersession MUST record its acting caller context and reason.
        //= spec/impl/v2/implementation.md#questions-preserve-their-posed-content-and-outcomes
        //# An open question MUST remain cancellable or supersedable by its current responsible record owner when the asker is inactive.
        QuestionMutation::Supersede{reason,replacement:posed,..} => {
            open(&q)?;text(reason,ctx.limits.text_bytes,true)?;
            if posed.record!=q.posed.record {return refuse("invalid_input","replacement must keep the linked record")}
            let value=insert_question(ctx,posed,Some(q.id.clone()))?;q.status=QuestionStatus::Superseded;q.superseded_by=Some(value.id.clone());
            action=Some(insert_action(ctx,&q,QuestionAction::Superseded{replacement_question_id:value.id.clone(),reason:reason.clone()})?);
            replacement=Some(view(ctx.tx,value,ctx.limits.record_bytes)?);
        }
        QuestionMutation::SupersedeAnswer{expected_answer_id,answer,reason,..} => {
            current_recipient(ctx,&q)?;answered(&q,expected_answer_id,false)?;text(reason,ctx.limits.text_bytes,true)?;
            let value=insert_answer(ctx,&q,answer,Some(expected_answer_id.clone()))?;
            q.latest_answer_id=Some(value.id.clone());q.active_answer_id=Some(value.id.clone());
            action=Some(insert_action(ctx,&q,QuestionAction::AnswerReplaced{previous_answer_id:expected_answer_id.clone(),answer_id:value.id.clone(),reason:reason.clone()})?);
            new_answer=Some(value);
        }
        QuestionMutation::WithdrawAnswer{expected_answer_id,reason,..} => {
            current_recipient(ctx,&q)?;answered(&q,expected_answer_id,true)?;text(reason,ctx.limits.text_bytes,true)?;
            q.active_answer_id=None;
            action=Some(insert_action(ctx,&q,QuestionAction::AnswerWithdrawn{answer_id:expected_answer_id.clone(),reason:reason.clone()})?);
        }
        //= spec/impl/v2/implementation.md#questions-preserve-their-posed-content-and-outcomes
        //# A moot mark MUST NOT create a product decision effect.
        //= spec/impl/v2/implementation.md#questions-preserve-their-posed-content-and-outcomes
        //# A moot mark MUST record its acting caller context and the answer that made the decision moot.
        //= spec/impl/v2/implementation.md#questions-preserve-their-posed-content-and-outcomes
        //# A moot mark MUST remove the marked answer's product effect.
        QuestionMutation::MarkMoot{expected_answer_id,caused_by,reason,..} => {
            answered(&q,expected_answer_id,true)?;text(reason,ctx.limits.text_bytes,true)?;
            get_answer(ctx.tx,caused_by,ctx.limits.record_bytes)?;
            q.status=QuestionStatus::Moot;q.active_answer_id=None;
            action=Some(insert_action(ctx,&q,QuestionAction::MarkedMoot{answer_id:expected_answer_id.clone(),caused_by:caused_by.clone(),reason:reason.clone()})?);
        }
        QuestionMutation::Pose{..} => return Err(Fault::Store("pose routed through mutation head".into())),
    }
    write_head(ctx,&before,&q)?;
    Ok(QuestionMutationOutcome{question:view(ctx.tx,q,ctx.limits.record_bytes)?,new_answer,replacement,action})
}

fn question_ref(q:&Question)->RecordRef { RecordRef::Question{question_id:q.id.clone()} }
fn answer_ref(q:&Question, id:&str)->AnswerRef { AnswerRef{question_id:q.id.clone(),answer_id:id.into()} }
fn full_text(text:&str)->BoundedText { BoundedText{text:text.into(),truncated:false} }
fn activity(q:&Question, change:QuestionChange)->WorkflowEffect {
    WorkflowEffect::Activity{subject:question_ref(q),subject_revision:q.revision,detail:ActivityDetail::Question(change)}
}
fn posed_effect(q:&Question)->WorkflowEffect {
    activity(q,QuestionChange::Posed{linked_record:q.posed.record.clone(),recipient:q.posed.recipient.clone(),prompt:full_text(&q.posed.prompt),recommended_option_id:q.posed.recommended_option_id.clone()})
}
/// The caller retains this return value and all staged effects in the same
/// operation receipt transaction. In particular, a refused resolver cascade
/// rolls back the question, answer, counters and activity together.
pub(super) fn apply(ctx:&Context<'_, '_>, mutation:&QuestionMutation)->Result<DomainChange<QuestionMutationOutcome>> {
    let outcome=transition(ctx,mutation)?;
    let q=&outcome.question.question;
    let mut effects=Vec::new();
    let resolution=match mutation {
        QuestionMutation::Pose{..}=>{ effects.push(posed_effect(q));None },
        QuestionMutation::Answer{answer,..}=>Some(QuestionResolution::Answered{question_id:q.id.clone(),answer:answer_ref(q,&outcome.new_answer.as_ref().ok_or_else(||Fault::Store("answer outcome absent".into()))?.id),selected_option_id:answer.option_id.clone()}),
        QuestionMutation::Cancel{reason,..}=>Some(QuestionResolution::Cancelled{question_id:q.id.clone(),action_revision:q.revision,reason:reason.clone()}),
        QuestionMutation::Supersede{reason,..}=>{
            let replacement=&outcome.replacement.as_ref().ok_or_else(||Fault::Store("replacement outcome absent".into()))?.question;
            effects.push(posed_effect(replacement));
            Some(QuestionResolution::Superseded{question_id:q.id.clone(),replacement_question_id:replacement.id.clone(),reason:reason.clone()})
        },
        QuestionMutation::SupersedeAnswer{expected_answer_id,answer,reason,..}=>{
            effects.push(activity(q,QuestionChange::AnswerReplaced{previous:answer_ref(q,expected_answer_id),current:answer_ref(q,&outcome.new_answer.as_ref().ok_or_else(||Fault::Store("replacement answer absent".into()))?.id),selected_option_id:answer.option_id.clone(),reason:full_text(reason)}));None
        },
        QuestionMutation::WithdrawAnswer{expected_answer_id,reason,..}=>{
            effects.push(activity(q,QuestionChange::AnswerWithdrawn{answer:answer_ref(q,expected_answer_id),reason:full_text(reason)}));None
        },
        QuestionMutation::MarkMoot{expected_answer_id,caused_by,reason,..}=>{
            effects.push(activity(q,QuestionChange::MarkedMoot{answer:answer_ref(q,expected_answer_id),caused_by:caused_by.clone(),reason:full_text(reason)}));None
        },
    };
    if let Some(resolution)=resolution {
        effects.push(activity(q,QuestionChange::Closed{resolution:resolution.clone()}));
        effects.push(WorkflowEffect::Resolve{resolution:ResolverOutcome::Question(resolution),requested_by:q.poser.clone(),linked_record:q.posed.record.clone()});
    }
    Ok(DomainChange{outcome,effects})
}

#[derive(Debug,Clone,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    database_id:String,scope:String,through:u64,position:(u64,String,String),
}
fn cursor(ctx:&QueryContext<'_, '_>,after:&Option<String>,scope:String)->Result<Cursor> {
    let database_id:String=ctx.tx.query_row("SELECT value FROM settings WHERE name='database_id'",[],|r|r.get(0))?;
    let current=product_revision(ctx.tx)?;
    let Some(raw)=after else{return Ok(Cursor{database_id,scope,through:current,position:(0,String::new(),String::new())})};
    if raw.len()>ctx.tx.limits.controller.collaboration_cursor_bytes {return refuse("invalid_input","cursor exceeds controller.collaboration_cursor_bytes")}
    let c:Cursor=serde_json::from_str(raw).map_err(|_|Fault::Refused("invalid_input","invalid question cursor".into()))?;
    if c.database_id!=database_id || c.scope!=scope || c.through>current || c.position.0==0 || c.position.0>c.through
        || !valid_id(ctx.tx.limits,&c.position.2) || !["question","answer","action","review"].contains(&c.position.1.as_str()) || encode(&c)?!=*raw {
        return refuse("invalid_input","question cursor differs from query or database")
    }
    Ok(c)
}
fn continuation(ctx:&QueryContext<'_, '_>,c:&Cursor,position:(u64,String,String))->Result<String> {
    let next=encode(&Cursor{position,..c.clone()})?;
    if next.len()>ctx.tx.limits.controller.collaboration_cursor_bytes {return refuse("response_bound","question cursor exceeds controller.collaboration_cursor_bytes")}
    Ok(next)
}
fn page_limit(ctx:&QueryContext<'_, '_>,limit:usize)->Result<i64> {
    if limit==0 || limit>ctx.limits.page_items {return refuse("input_bound","question page exceeds workflow.page_items or is zero")}
    integer(u64::try_from(limit.checked_add(1).ok_or_else(||Fault::Store("page representation overflow".into()))?).map_err(|_|Fault::Store("page representation overflow".into()))?)
}
fn pack<T:Serialize>(ctx:&QueryContext<'_, '_>,c:&Cursor,rows:Vec<((u64,String,String),T)>,limit:usize)->Result<Page<T>> {
    let more=rows.len()>limit;let mut page=Page{items:vec![],next_cursor:None};let mut last=None;
    for (position,item) in rows.into_iter().take(limit) {
        page.items.push(item);page.next_cursor=Some(continuation(ctx,c,position)?);
        if bounded_json_size(&page,ctx.limits.page_bytes).is_err() {
            page.items.pop();
            if page.items.is_empty(){return refuse("response_bound","question item exceeds workflow.page_bytes")}
            page.next_cursor=last;return Ok(page)
        }
        last=page.next_cursor.clone();
    }
    page.next_cursor=if more{last}else{None};
    bounded_json_size(&page,ctx.limits.page_bytes).map_err(|_|Fault::Refused("response_bound","question page exceeds workflow.page_bytes".into()))?;
    Ok(page)
}
fn list(ctx:&QueryContext<'_, '_>,record:&Option<RecordRef>,selected:&Option<QuestionStatus>,after:&Option<String>,limit:usize)->Result<Page<QuestionView>> {
    let row_limit=page_limit(ctx,limit)?;
    if let Some(record)=record {id(ctx.tx,record_key(record).1)?;record_owner(ctx.tx,record)?;}
    let c=cursor(ctx,after,encode(&("questions",record,selected))?)?;
    let (kind,record_id)=record.as_ref().map(record_key).map(|(k,id)|(Some(k),Some(id))).unwrap_or((None,None));
    let selected=selected.as_ref().map(status);
    let mut statement=ctx.tx.prepare("SELECT id,created_product_revision FROM workflow_questions WHERE (?1 IS NULL OR (record_kind=?1 AND record_id=?2)) AND (?3 IS NULL OR status=?3) AND created_product_revision<=?4 AND (created_product_revision,'question',id)>(?5,?6,?7) ORDER BY created_product_revision,id LIMIT ?8")?;
    let rows=statement.query_map(params![kind,record_id,selected,integer(c.through)?,integer(c.position.0)?,c.position.1,c.position.2,row_limit],|r|Ok((r.get::<_,String>(0)?,r.get::<_,i64>(1)?)))?.collect::<std::result::Result<Vec<_>,_>>()?;
    let rows=rows.into_iter().map(|(id,created)|Ok(((unsigned(created)?,"question".into(),id.clone()),view(ctx.tx,get(ctx.tx,&id,ctx.limits.record_bytes)?,ctx.limits.record_bytes)?))).collect::<Result<Vec<_>>>()?;
    pack(ctx,&c,rows,limit)
}
fn answers(ctx:&QueryContext<'_, '_>,question_id:&str,after:&Option<String>,limit:usize)->Result<Page<RecordedAnswer>> {
    get(ctx.tx,question_id,ctx.limits.record_bytes)?;let row_limit=page_limit(ctx,limit)?;
    let c=cursor(ctx,after,encode(&("question_answers",question_id))?)?;
    let mut statement=ctx.tx.prepare("SELECT id,product_revision FROM workflow_answers WHERE question_id=?1 AND product_revision<=?2 AND (product_revision,'answer',id)>(?3,?4,?5) ORDER BY product_revision,id LIMIT ?6")?;
    let rows=statement.query_map(params![question_id,integer(c.through)?,integer(c.position.0)?,c.position.1,c.position.2,row_limit],|r|Ok((r.get::<_,String>(0)?,r.get::<_,i64>(1)?)))?.collect::<std::result::Result<Vec<_>,_>>()?;
    let rows=rows.into_iter().map(|(id,revision)|Ok(((unsigned(revision)?,"answer".into(),id.clone()),get_answer(ctx.tx,&AnswerRef{question_id:question_id.into(),answer_id:id},ctx.limits.record_bytes)?))).collect::<Result<Vec<_>>>()?;
    pack(ctx,&c,rows,limit)
}
fn actions(ctx:&QueryContext<'_, '_>,question_id:&str,after:&Option<String>,limit:usize)->Result<Page<RecordedQuestionAction>> {
    get(ctx.tx,question_id,ctx.limits.record_bytes)?;let row_limit=page_limit(ctx,limit)?;
    let c=cursor(ctx,after,encode(&("question_actions",question_id))?)?;
    let mut statement=ctx.tx.prepare("SELECT question_revision,product_revision,CAST(substr(CAST(body AS BLOB),1,?6) AS TEXT) FROM workflow_question_actions WHERE question_id=?1 AND product_revision<=?2 AND (product_revision,'action',printf('%020d',question_revision))>(?3,?4,?5) ORDER BY product_revision,question_revision LIMIT ?7")?;
    let rows=statement.query_map(params![question_id,integer(c.through)?,integer(c.position.0)?,c.position.1,c.position.2,super::database::sql_bound(ctx.limits.record_bytes.saturating_add(1))?,row_limit],|r|Ok((r.get::<_,i64>(0)?,r.get::<_,i64>(1)?,r.get::<_,String>(2)?)))?.collect::<std::result::Result<Vec<_>,_>>()?;
    let rows=rows.into_iter().map(|(revision,product,raw)|{
        let value:RecordedQuestionAction=stored(&raw,ctx.limits.record_bytes)?;validate_recorded_actor(ctx.tx,&value.actor)?;
        if value.question_id!=question_id || value.question_revision!=unsigned(revision)? || value.product_revision!=unsigned(product)? {return Err(Fault::Store("question action/index mismatch".into()))}
        Ok(((value.product_revision,"action".into(),format!("{:020}",value.question_revision)),value))
    }).collect::<Result<Vec<_>>>()?;
    pack(ctx,&c,rows,limit)
}
fn queue_position(item:&DecisionQueueItem)->(u64,String,String) {
    match item {
        DecisionQueueItem::Question{created_product_revision,question_id,..}=>(*created_product_revision,"question".into(),question_id.clone()),
        DecisionQueueItem::Review{created_product_revision,review_id,..}=>(*created_product_revision,"review".into(),review_id.clone()),
    }
}
fn decisions(ctx:&QueryContext<'_, '_>,viewer:&PrincipalRef,after:&Option<String>,limit:usize)->Result<DecisionQueuePage> {
    if ctx.actor.principal!=PrincipalRef::TrustedHostOperator && ctx.actor.principal!=*viewer {return refuse("recipient_mismatch","agent decision queue is bound to the current caller")}
    recipient(ctx.tx,viewer)?;let row_limit=page_limit(ctx,limit)?;
    let c=cursor(ctx,after,encode(&("decision_queue",viewer))?)?;
    // Delivery cursors and notice acknowledgements cannot replace canonical
    // open-record membership in either half of this query.
    //= spec/impl/v2/implementation.md#the-supported-decision-queue-is-complete
    //# A notice outside the decision queue MUST NOT substitute for an owed decision's queue entry.
    //= spec/impl/v2/implementation.md#the-supported-decision-queue-is-complete
    //# The decision queue MUST include every open question directly routed to its viewer.
    let mut statement=ctx.tx.prepare("SELECT id FROM workflow_questions WHERE recipient=?1 AND status='open' AND created_product_revision<=?2 AND (created_product_revision,'question',id)>(?3,?4,?5) ORDER BY created_product_revision,id LIMIT ?6")?;
    let ids=statement.query_map(params![encode(viewer)?,integer(c.through)?,integer(c.position.0)?,c.position.1,c.position.2,row_limit],|r|r.get::<_,String>(0))?.collect::<std::result::Result<Vec<_>,_>>()?;
    let mut rows=Vec::new();
    for id in ids {
        let q=get(ctx.tx,&id,ctx.limits.record_bytes)?;
        rows.push(DecisionQueueItem::Question{question_id:q.id,revision:q.revision,created_product_revision:q.created_product_revision,record:q.posed.record,recipient:q.posed.recipient,prompt:q.posed.prompt,recommended_option_id:q.posed.recommended_option_id});
    }
    let review_after=(c.position.0,if c.position.1=="review" {c.position.2.clone()}else{String::new()});
    //= spec/impl/v2/implementation.md#the-supported-decision-queue-is-complete
    //# The decision queue MUST include every open review directly routed to its viewer.
    rows.extend(super::collaboration_reviews::pending_review_rows(ctx,viewer,Some(review_after),c.through,row_limit as usize)?);
    rows.sort_by_key(queue_position);
    //= spec/impl/v2/implementation.md#the-supported-decision-queue-is-complete
    //# A bounded decision-queue page MUST provide a continuation when more matching decisions remain.
    let more=rows.len()>limit;let mut page=DecisionQueuePage{viewer:viewer.clone(),items:vec![],through_product_revision:c.through,observed_product_revision:product_revision(ctx.tx)?,next_cursor:None};let mut last=None;
    for item in rows.into_iter().take(limit) {
        let position=queue_position(&item);page.items.push(item);page.next_cursor=Some(continuation(ctx,&c,position)?);
        if bounded_json_size(&page,ctx.limits.page_bytes).is_err() {
            page.items.pop();if page.items.is_empty(){return refuse("response_bound","decision queue item exceeds workflow.page_bytes")}
            page.next_cursor=last;return Ok(page)
        }
        last=page.next_cursor.clone();
    }
    page.next_cursor=if more{last}else{None};
    bounded_json_size(&page,ctx.limits.page_bytes).map_err(|_|Fault::Refused("response_bound","decision queue exceeds workflow.page_bytes".into()))?;
    Ok(page)
}
pub(super) fn query(ctx:&QueryContext<'_, '_>,query:&QuestionQuery)->Result<QuestionQueryResult> {
    Ok(match query {
        QuestionQuery::Get{question_id}=>QuestionQueryResult::Question(view(ctx.tx,get(ctx.tx,question_id,ctx.limits.record_bytes)?,ctx.limits.record_bytes)?),
        QuestionQuery::List{record,status,after,limit}=>QuestionQueryResult::Questions(list(ctx,record,status,after,*limit)?),
        QuestionQuery::Answers{question_id,after,limit}=>QuestionQueryResult::Answers(answers(ctx,question_id,after,*limit)?),
        QuestionQuery::Actions{question_id,after,limit}=>QuestionQueryResult::Actions(actions(ctx,question_id,after,*limit)?),
        QuestionQuery::DecisionQueue{viewer,after,limit}=>QuestionQueryResult::DecisionQueue(decisions(ctx,viewer,after,*limit)?),
    })
}

/// Common comment activity may advance this aggregate CAS head without changing
/// its immutable posed content, answer effect or resolver settlement.
pub(super) fn touch(ctx:&Context<'_, '_>,question_id:&str,expected_revision:u64)->Result<u64> {
    let before=get(ctx.tx,question_id,ctx.limits.record_bytes)?;
    if before.revision!=expected_revision{return refuse("stale_revision","question revision differs")}
    let mut after=before.clone();after.revision=after.revision.checked_add(1).ok_or_else(||Fault::Store("question revision exhausted".into()))?;
    after.changed_product_revision=ctx.product_revision;integer(after.revision)?;write_head(ctx,&before,&after)?;Ok(after.revision)
}

/// Root calls this from its existing schema/content validation before admission.
/// Iterate bounded batches; old answers/actions remain mandatory stored content,
/// even after their effect was replaced, withdrawn or made moot.
pub(super) fn validate_stored(tx:&StoreContext<'_>,limits:&crate::limits::WorkflowLimits)->Result<()> {
    let mut after=String::new();
    loop {
        let mut stmt=tx.prepare("SELECT id FROM workflow_questions WHERE id>?1 ORDER BY id LIMIT ?2")?;
        let ids=stmt.query_map(params![after,super::database::sql_bound(limits.page_items)?],|r|r.get::<_,String>(0))?.collect::<std::result::Result<Vec<_>,_>>()?;
        if ids.is_empty(){break}
        for question_id in ids {
            let q=get(tx,&question_id,limits.record_bytes)?;record_owner(tx,&q.posed.record)?;
            recipient(tx,&q.posed.recipient)?;text(&q.posed.prompt,limits.text_bytes,true)?;
            if q.posed.options.is_empty()||q.posed.options.len()>limits.options{return Err(Fault::Store("stored question option count invalid".into()))}
            let mut option_ids=BTreeSet::new();
            for option in &q.posed.options {id(tx,&option.id)?;text(&option.label,limits.text_bytes,true)?;text(&option.explanation,limits.text_bytes,false)?;if !option_ids.insert(&option.id){return Err(Fault::Store("stored duplicate question option".into()))}}
            if !option_ids.contains(&q.posed.recommended_option_id){return Err(Fault::Store("stored question recommendation invalid".into()))}
            if !q.posed.prompt.ends_with(&recommendation_suffix(&q.posed)?) {return Err(Fault::Store("stored prompt does not state its selected recommendation".into()))}
            if let Some(previous)=&q.supersedes {let previous=get(tx,previous,limits.record_bytes)?;if previous.superseded_by.as_deref()!=Some(q.id.as_str())||previous.created_product_revision>=q.created_product_revision||previous.posed.record!=q.posed.record{return Err(Fault::Store("replacement question lineage mismatch".into()))}}
            if let Some(next)=&q.superseded_by {let next=get(tx,next,limits.record_bytes)?;if next.supersedes.as_deref()!=Some(q.id.as_str())||q.created_product_revision>=next.created_product_revision||next.posed.record!=q.posed.record{return Err(Fault::Store("superseded question lineage mismatch".into()))}}
            validate_lineage(tx,&q,limits)?;
            after=question_id;
        }
    }
    // Foreign keys are also checked by the root; these joins refuse orphan data
    // even when opening a damaged database with foreign key enforcement disabled.
    let orphan:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM workflow_answers a LEFT JOIN workflow_questions q ON q.id=a.question_id WHERE q.id IS NULL) OR EXISTS(SELECT 1 FROM workflow_question_actions a LEFT JOIN workflow_questions q ON q.id=a.question_id WHERE q.id IS NULL)",[],|r|r.get(0))?;
    if orphan{return Err(Fault::Store("orphaned question history".into()))}Ok(())
}

/// Reconstruct effect state from the immutable answer chain and action history.
/// Comments may add aggregate revisions between actions; they cannot move the
/// answer head backwards or resurrect a withdrawn/moot effect. Scan in bounded
/// pages and keep only the first/prior answer plus current derived state.
fn validate_lineage(tx:&StoreContext<'_>,q:&Question,limits:&crate::limits::WorkflowLimits)->Result<()> {
    let fail=||Fault::Store("question head differs from immutable answer/action lineage".into());
    let mut after_product=0u64;
    let mut answer_count=0usize;
    let mut first:Option<RecordedAnswer>=None;
    let mut prior:Option<RecordedAnswer>=None;
    loop {
        let mut stmt=tx.prepare("SELECT id,product_revision FROM workflow_answers WHERE question_id=?1 AND product_revision>?2 ORDER BY product_revision,id LIMIT ?3")?;
        let ids=stmt.query_map(params![q.id,integer(after_product)?,super::database::sql_bound(limits.page_items)?],|r|Ok((r.get::<_,String>(0)?,r.get::<_,i64>(1)?)))?.collect::<std::result::Result<Vec<_>,_>>()?;
        if ids.is_empty(){break}
        for(id,product)in ids {
            let a=get_answer(tx,&AnswerRef{question_id:q.id.clone(),answer_id:id},limits.record_bytes)?;
            if a.product_revision!=unsigned(product)? || a.product_revision<=after_product
                || a.product_revision<=q.created_product_revision || a.product_revision>q.changed_product_revision
                || a.author.principal!=q.posed.recipient || !valid_id(tx.limits,&a.operation_id)
                || !q.posed.options.iter().any(|o|o.id==a.answer.option_id)
                || a.replaces.as_deref()!=prior.as_ref().map(|a|a.id.as_str()) {return Err(fail())}
            text(&a.answer.explanation,limits.text_bytes,false)?;
            if first.is_none(){first=Some(a.clone())}
            after_product=a.product_revision;prior=Some(a);answer_count+=1;
        }
    }
    // Duplicate product identities cannot hide across page boundaries.
    let stored_count:i64=tx.query_row("SELECT COUNT(*) FROM workflow_answers WHERE question_id=?1",[&q.id],|r|r.get(0))?;
    if usize::try_from(stored_count).ok()!=Some(answer_count){return Err(fail())}
    let mut derived=QuestionStatus::Open;
    let mut latest:Option<String>=None;
    let mut active:Option<String>=None;
    let mut superseded:Option<String>=None;
    let mut first_consumed=false;
    let mut replacements=0usize;
    let mut action_after=0u64;
    let mut product_after=q.created_product_revision;
    loop {
        let mut stmt=tx.prepare("SELECT question_revision,product_revision,CAST(substr(CAST(body AS BLOB),1,?3) AS TEXT) FROM workflow_question_actions WHERE question_id=?1 AND question_revision>?2 ORDER BY question_revision LIMIT ?4")?;
        let rows=stmt.query_map(params![q.id,integer(action_after)?,super::database::sql_bound(limits.record_bytes.saturating_add(1))?,super::database::sql_bound(limits.page_items)?],|r|Ok((r.get::<_,i64>(0)?,r.get::<_,i64>(1)?,r.get::<_,String>(2)?)))?.collect::<std::result::Result<Vec<_>,_>>()?;
        if rows.is_empty(){break}
        for(rev,product,raw)in rows {
            let a:RecordedQuestionAction=stored(&raw,limits.record_bytes)?;
            validate_recorded_actor(tx,&a.actor)?;
            if a.question_id!=q.id || a.question_revision!=unsigned(rev)? || a.question_revision<=action_after
                || a.question_revision<2 || a.question_revision>q.revision || a.product_revision!=unsigned(product)?
                || a.product_revision<=product_after || a.product_revision>q.changed_product_revision
                || !valid_id(tx.limits,&a.operation_id) {return Err(fail())}
            if !first_consumed {
                if let Some(initial)=&first {
                    if initial.product_revision==a.product_revision {return Err(fail())}
                    if initial.product_revision<a.product_revision {
                        if derived!=QuestionStatus::Open {return Err(fail())}
                        latest=Some(initial.id.clone());active=latest.clone();derived=QuestionStatus::Answered;first_consumed=true;
                    }
                }
            }
            match &a.action {
                QuestionAction::Cancelled{reason}=>{
                    text(reason,limits.text_bytes,true)?;
                    if derived!=QuestionStatus::Open{return Err(fail())}
                    derived=QuestionStatus::Cancelled;
                }
                QuestionAction::Superseded{replacement_question_id,reason}=>{
                    text(reason,limits.text_bytes,true)?;
                    if derived!=QuestionStatus::Open{return Err(fail())}
                    let next=get(tx,replacement_question_id,limits.record_bytes)?;
                    if next.supersedes.as_deref()!=Some(q.id.as_str()) || next.created_product_revision!=a.product_revision
                        || next.poser!=a.actor || next.posed.record!=q.posed.record {return Err(fail())}
                    derived=QuestionStatus::Superseded;superseded=Some(replacement_question_id.clone());
                }
                QuestionAction::AnswerReplaced{previous_answer_id,answer_id,reason}=>{
                    text(reason,limits.text_bytes,true)?;
                    if derived!=QuestionStatus::Answered || latest.as_deref()!=Some(previous_answer_id.as_str()) {return Err(fail())}
                    let answer=get_answer(tx,&AnswerRef{question_id:q.id.clone(),answer_id:answer_id.clone()},limits.record_bytes)?;
                    if answer.replaces.as_deref()!=Some(previous_answer_id.as_str()) || answer.product_revision!=a.product_revision
                        || answer.operation_id!=a.operation_id || answer.author!=a.actor || a.actor.principal!=q.posed.recipient {return Err(fail())}
                    latest=Some(answer_id.clone());active=latest.clone();replacements+=1;
                }
                QuestionAction::AnswerWithdrawn{answer_id,reason}=>{
                    text(reason,limits.text_bytes,true)?;
                    if derived!=QuestionStatus::Answered || active.as_deref()!=Some(answer_id.as_str()) || a.actor.principal!=q.posed.recipient {return Err(fail())}
                    active=None;
                }
                QuestionAction::MarkedMoot{answer_id,caused_by,reason}=>{
                    text(reason,limits.text_bytes,true)?;
                    if derived!=QuestionStatus::Answered || active.as_deref()!=Some(answer_id.as_str()) {return Err(fail())}
                    let cause=get_answer(tx,caused_by,limits.record_bytes)?;
                    if cause.product_revision>=a.product_revision {return Err(fail())}
                    active=None;derived=QuestionStatus::Moot;
                }
            }
            action_after=a.question_revision;product_after=a.product_revision;
        }
    }
    if !first_consumed {
        if let Some(initial)=&first {
            if derived!=QuestionStatus::Open || initial.product_revision<=product_after {return Err(fail())}
            latest=Some(initial.id.clone());active=latest.clone();derived=QuestionStatus::Answered;first_consumed=true;
        }
    }
    if answer_count!=replacements+usize::from(first_consumed) || q.status!=derived || q.latest_answer_id!=latest
        || q.active_answer_id!=active || q.superseded_by!=superseded {return Err(fail())}
    Ok(())
}

#[cfg(test)]
mod index_bound_tests {
    use super::*;

    // Direct real-SQLite reader discriminator; this deliberately inserted row
    // is not a claim of canonical task acceptance or startup integration.
    #[test]
    //= spec/impl/v2/implementation.md#durable-commands-have-stable-identity
    //= type=test
    //# Corrupt stored task state MUST be refused without rewriting its data.
    fn question_indexes_are_byte_bounded_before_canonical_body_decode() {
        let p=crate::limits::RuntimeLimits::seed().unwrap();
        let sql=rusqlite::Connection::open_in_memory().unwrap();
        sql.execute_batch("CREATE TABLE workflow_questions(id TEXT PRIMARY KEY,revision INTEGER,created_product_revision INTEGER,recipient TEXT,status TEXT,record_kind TEXT,record_id TEXT,body TEXT)").unwrap();
        let tx=StoreContext::new(&sql,&p);
        let q=Question{id:"question".into(),revision:1,created_product_revision:1,changed_product_revision:1,
            posed:PoseQuestion{record:RecordRef::Task{task_id:"task".into()},recipient:PrincipalRef::TrustedHostOperator,
                prompt:"Choose.\n\nRecommended option: \"keep\" — \"Keep\".".into(),
                options:vec![QuestionOption{id:"keep".into(),label:"Keep".into(),explanation:String::new()}],recommended_option_id:"keep".into()},
            poser:RecordedActor{principal:PrincipalRef::TrustedHostOperator,provenance:ActorProvenance::TrustedHost{path:TrustedHostPath::OperatorApi}},
            status:QuestionStatus::Open,latest_answer_id:None,active_answer_id:None,superseded_by:None,supersedes:None};
        let body=encode(&q).unwrap();let maximum=body.len();
        let recipient=encode(&q.posed.recipient).unwrap();
        sql.execute("INSERT INTO workflow_questions VALUES('question',1,1,?1,'open','task','task',?2)",params![recipient,body]).unwrap();
        assert_eq!(get(&tx,"question",maximum).unwrap(),q); // Exact body bound is allowed.
        let original=[recipient,"open".into(),"task".into(),"task".into()];
        for (column,index) in ["recipient","status","record_kind","record_id"].into_iter().enumerate() {
            // A NUL-prefixed suffix must not be hidden by SQLite TEXT substr.
            let oversized=format!("{}\0{}",original[column],"x".repeat(maximum+1));
            for raw in [body.as_str(),"{"] {
                let mut indexes=original.clone();indexes[column]=oversized.clone();
                sql.execute("UPDATE workflow_questions SET recipient=?1,status=?2,record_kind=?3,record_id=?4,body=?5 WHERE id='question'",
                    params![indexes[0],indexes[1],indexes[2],indexes[3],raw]).unwrap();
                let changes=sql.total_changes();
                let result=get(&tx,"question",maximum);
                assert!(matches!(result,Err(Fault::Store(ref message)) if message=="stored question index exceeds configured workflow.record_bytes"),"{index}: {result:?}");
                let after:(String,String,String,String,String)=sql.query_row("SELECT recipient,status,record_kind,record_id,body FROM workflow_questions WHERE id='question'",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))).unwrap();
                assert_eq!(after,(indexes[0].clone(),indexes[1].clone(),indexes[2].clone(),indexes[3].clone(),raw.to_owned()));
                assert_eq!(sql.total_changes(),changes);
            }
        }
        // An under-bound index mismatch remains an identity refusal, not a
        // successful prefix comparison or an invented recipient/owner change.
        for state in ["cancelled","open\0ignored"] {
            sql.execute("UPDATE workflow_questions SET recipient=?1,status=?2,record_kind='task',record_id='task',body=?3 WHERE id='question'",params![original[0],state,body]).unwrap();
            assert!(matches!(get(&tx,"question",maximum),Err(Fault::Store(ref message)) if message=="question/index identity mismatch"));
        }
        sql.execute("UPDATE workflow_questions SET status='open' WHERE id='question'",[]).unwrap();
        assert_eq!(get(&tx,"question",maximum).unwrap(),q);
    }
}
