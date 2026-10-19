//! Completion feedback uses the existing mutation journal and original receipts.
//! A retained Begin is input intent, never evidence that an executor consumed it.
use super::*;
use crate::governance::{CompletionDisposition, DecisionPoint, DecisionRef};
use crate::governance_completion::*;
use crate::mcp::Caller;
use std::io::Read;

pub(super) enum Gate {
    Admit,
    Blocked(CompletionEvaluation),
    Held,
}

pub(super) fn required_events(evaluation: &CompletionEvaluation,
    reset: Option<&crate::collaboration::workflow::EventId>) -> Vec<crate::collaboration::workflow::EventId> {
    // A reset-based successor must carry its exact admission basis. Historical
    // reasons may span other pages and cannot substitute for this occurrence.
    if let Some(event)=reset {return vec![event.clone()];}
    let mut events: Vec<_>=evaluation.state.reasons.iter().filter_map(|reason| match reason {
        crate::governance::CompletionReason::UnconsumedInput {event_id}=>Some(event_id.clone()), _=>None,
    }).collect();
    events.sort(); events.dedup(); events
}

pub(super) struct Host {
    client: ApiClient,
    caller: Caller,
    session_id: String,
    directory: PathBuf,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct InputRecord {
    revision: u32,
    feedback: CompletionFeedback,
    text: String,
    started: bool,
    accepted_at: Option<CompletionBoundary>,
    consumed_at: Option<CompletionBoundary>,
    #[serde(default)]
    native_input: Option<NativeCompletionInput>,
    #[serde(default,skip_serializing_if="Vec::is_empty")]
    supplied_retries: Vec<CompletionBoundary>,
}

pub(super) struct Input {
    directory: PathBuf,
    record: InputRecord,
}

impl Input {
    pub(super) fn text(&self) -> &str { &self.record.text }
    pub(super) fn feedback(&self) -> &CompletionFeedback { &self.record.feedback }
    pub(super) fn delivery_batch_id(&self) -> Option<&str> {
        self.record.feedback.continuation.delivery_batch_id.as_deref()
    }
    /// Only the live caller of C's accepted original rejection operation may
    /// advance this chain. The dispatch marker still forbids restart resends.
    pub(super) fn record_native_rejection(&mut self, binding: &crate::native::ProviderRequestBinding,
        rejection: NativeCompletionRejection, request_limit: u32, request_bytes: usize,
        limits: &RuntimeLimits) -> Result<(), String> {
        let path = self.directory.join("input.json");
        if read_input(&path,limits)?.as_ref() != Some(&self.record) || !self.record.started
            || self.record.consumed_at.is_some() || self.record.accepted_at.is_some() {
            return Err("completion native rejection does not bind the retained live input".into());
        }
        let mut proof = self.record.native_input.clone().unwrap_or_else(||NativeCompletionInput {
            request_binding:binding.clone(),rejections:Vec::new(),
        });
        if proof.request_binding != *binding {
            return Err("completion native retry input binding differs".into());
        }
        validate_native_input(&self.record.feedback.continuation.target,&proof,rejection.request_index,
            request_limit,request_bytes,limits)?;
        let policy=limits.governance.as_ref().ok_or("completion policy is absent")?;
        if proof.rejections.len() >= policy.scan_rows || rejection.request_index >= request_limit
            || !crate::mcp::valid_identity(&rejection.operation_id,limits.transport.identity_bytes)
            || proof.rejections.iter().any(|old|old.operation_id == rejection.operation_id) {
            return Err("completion native rejection chain exceeds its accepted bounds".into());
        }
        proof.rejections.push(rejection);
        bounded_json_size(&proof,policy.record_bytes)
            .map_err(|_|"completion native input exceeds governance.record_bytes")?;
        self.record.native_input=Some(proof);
        persist_input(&path,&self.record,limits)
    }

    /// Called only after a complete validated actual response, never a 429 or
    /// an exposed/unknown request. No canonical target or prior receipt changes.
    pub(super) fn native_consumption(&self, binding: &crate::native::ProviderRequestBinding,
        request_index:u32,request_limit:u32,request_bytes:usize,limits:&RuntimeLimits)
        ->Result<NativeCompletionInput,String> {
        let proof=self.record.native_input.clone().unwrap_or_else(||NativeCompletionInput {
            request_binding:binding.clone(),rejections:Vec::new(),
        });
        if !self.record.started || proof.request_binding != *binding {
            return Err("completion native response input binding differs".into());
        }
        validate_native_input(&self.record.feedback.continuation.target,&proof,request_index,
            request_limit,request_bytes,limits)?;
        Ok(proof)
    }

    /// Retain the actual turn/start identity before accepting its terminal event.
    pub(super) fn record_turn_start(&mut self, boundary: CompletionBoundary, limits: &RuntimeLimits) -> Result<(), String> {
        if !self.record.started || self.record.accepted_at.is_some()
            || !matches!(&boundary, CompletionBoundary::CodexTurn {..})
            || !consumes(&self.record.feedback.continuation.target,&boundary) {
            return Err("completion continuation start association differs".into());
        }
        self.record.accepted_at = Some(boundary);
        persist_input(&self.directory.join("input.json"),&self.record,limits)
    }
    pub(super) fn record_supplied_retry(&mut self,thread:&str,previous:&str,next:&str,limits:&RuntimeLimits)->Result<(),String> {
        let old=CompletionBoundary::CodexTurn{thread_id:thread.into(),turn_id:previous.into()};
        if !self.record.started || self.record.consumed_at.is_some()
            || self.record.supplied_retries.last().or(self.record.accepted_at.as_ref())!=Some(&old)
            || previous==next || self.record.supplied_retries.len()>=limits.governance.as_ref().ok_or("completion policy absent")?.scan_rows {
            return Err("completion rate continuation differs from original input lineage".into());
        }
        self.record.supplied_retries.push(CompletionBoundary::CodexTurn{thread_id:thread.into(),turn_id:next.into()});
        persist_input(&self.directory.join("input.json"),&self.record,limits)
    }
    /// This durable before-send barrier can be crossed once by the live owner.
    /// A visible marker after a failed sync is conservatively treated as used.
    pub(super) fn mark_dispatch(&mut self, limits: &RuntimeLimits) -> Result<(), String> {
        let path = self.directory.join("input.json");
        let old = read_input(&path, limits)?.ok_or("completion input record missing")?;
        if old != self.record || old.started || old.consumed_at.is_some() {
            return Err("completion input dispatch already began or differs; no resend".into());
        }
        self.record.started = true;
        persist_input(&path, &self.record, limits)
    }
}

fn key(boundary: &impl Serialize) -> Result<String, String> {
    let bytes = serde_json::to_vec(boundary).map_err(|_| "completion boundary encoding failed")?;
    let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, &bytes);
    Ok(digest.as_ref().iter().map(|b| format!("{b:02x}")).collect())
}

fn operation_id(operation: &Operation) -> Result<&str, String> {
    match operation {
        Operation::EvaluateCompletion(input) => Ok(&input.operation_id),
        Operation::BeginCompletionContinuation(input) => Ok(&input.operation_id),
        Operation::AcknowledgeCompletionInput(input) => Ok(&input.operation_id),
        _ => Err("completion journal has another operation".into()),
    }
}

fn read_input(path: &Path, limits: &RuntimeLimits) -> Result<Option<InputRecord>, String> {
    let file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err("completion input record unavailable".into()),
    };
    let mut bytes = Vec::new();
    file.take(limits.worker.mutation_record_bytes as u64 + 1).read_to_end(&mut bytes)
        .map_err(|_| "completion input record unreadable")?;
    if bytes.len() > limits.worker.mutation_record_bytes {
        return Err("completion input exceeds worker.mutation_record_bytes".into());
    }
    let value: InputRecord = serde_json::from_slice(&bytes)
        .map_err(|_| "completion input record invalid")?;
    if value.revision != 1 || (value.consumed_at.is_some() || value.accepted_at.is_some()) && !value.started {
        return Err("completion input record state invalid".into());
    }
    Ok(Some(value))
}

fn persist_input(path: &Path, value: &InputRecord, limits: &RuntimeLimits) -> Result<(), String> {
    bounded_json_size(value, limits.worker.mutation_record_bytes)
        .map_err(|_| "completion input exceeds worker.mutation_record_bytes")?;
    atomic_json(path, value)
}

pub(super) fn admit_prior_supplied_inputs(state:&Path,limits:&RuntimeLimits)->Result<(),String> {
    let root=state.join("completion");
    let callers=match std::fs::read_dir(&root) {
        Ok(rows)=>rows,Err(e) if e.kind()==std::io::ErrorKind::NotFound=>return Ok(()),
        Err(_)=>return Err("completion inventory unavailable".into()),
    };
    let bound=limits.governance.as_ref().ok_or("retained completion lacks selected policy")?.scan_rows;
    let mut count=0usize;
    for caller in callers {
        count=count.checked_add(1).ok_or("completion inventory overflow")?;
        if count>bound{return Err("completion inventory exceeds governance.scan_rows".into());}
        let path=caller.map_err(|_|"completion inventory unavailable")?.path();
        if !std::fs::symlink_metadata(&path).map_err(|_|"completion inventory unavailable")?.is_dir(){return Err("unexpected completion caller entry".into());}
        for boundary in std::fs::read_dir(path).map_err(|_|"completion boundary inventory unavailable")? {
            count=count.checked_add(1).ok_or("completion inventory overflow")?;
            if count>bound{return Err("completion inventory exceeds governance.scan_rows".into());}
            let path=boundary.map_err(|_|"completion boundary unavailable")?.path();
            if !std::fs::symlink_metadata(&path).map_err(|_|"completion boundary unavailable")?.is_dir(){return Err("unexpected completion boundary entry".into());}
            if read_input(&path.join("input.json"),limits)?.is_some_and(|v|!v.supplied_retries.is_empty()) {
                return Err("supplied completion lineage requires worker19".into());
            }
        }
    }
    Ok(())
}


// Traceability scope: The permitted continuation receives the original bounded decision and recorded remaining-work reasons.
//= spec/impl/v2/implementation.md#voluntary-completion-checks-remaining-work
//# A permitted continuation after refused voluntary completion MUST receive the recorded remaining-work reason as input.

fn feedback_text(feedback: &CompletionFeedback, limits: &RuntimeLimits) -> Result<String, String> {
    let policy = limits.governance.as_ref().ok_or("completion policy is absent")?;
    if feedback.evaluation.state.reasons.is_empty()
        || feedback.evaluation.state.reasons.len() > policy.reason_items {
        return Err("completion feedback reason count differs from governance bounds".into());
    }
    bounded_json_size(&feedback.evaluation.state.reasons,policy.reason_bytes)
        .map_err(|_| "completion reasons exceed governance.reason_bytes")?;
    let body = json!({"decision":feedback.evaluation.decision,
        "remaining_work":feedback.evaluation.state.reasons});
    let text = format!("The controller refused voluntary completion. Address this recorded remaining work within the existing task authority and limits:\n{}",
        serde_json::to_string(&body).map_err(|_| "completion feedback encoding failed")?);
    if text.len() > policy.record_bytes {
        return Err("completion feedback exceeds governance.record_bytes".into());
    }
    if value_contains_credential(&serde_json::to_value(feedback).map_err(|_| "completion feedback encoding failed")?,
        &configured_credentials(limits)?, &limits.worker)? {
        return Err("completion feedback contains a configured credential".into());
    }
    Ok(text)
}

fn consumes(target: &CompletionTarget, boundary: &CompletionBoundary) -> bool {
    match (target, boundary) {
        (CompletionTarget::NativeRequest {request_index:a}, CompletionBoundary::NativeResponse {request_index:b}) => a == b,
        (CompletionTarget::InitialNativeRequest {request_index:a}, CompletionBoundary::NativeResponse {request_index:b}) => *a == 1 && a == b,
        (CompletionTarget::InitialCodexTurn {thread_id:a}, CompletionBoundary::CodexTurn {thread_id:b,turn_id}) => a == b && !turn_id.is_empty(),
        (CompletionTarget::CodexTurn {thread_id:a,previous_turn_id}, CompletionBoundary::CodexTurn {thread_id:b,turn_id}) => a == b && previous_turn_id != turn_id,
        _ => false,
    }
}

fn native_target(target:&CompletionTarget)->Option<u32> {
    match target {
        CompletionTarget::NativeRequest {request_index} if *request_index > 0=>Some(*request_index),
        CompletionTarget::InitialNativeRequest {request_index:1}=>Some(1),
        _=>None,
    }
}

fn native_consumes(target:&CompletionTarget,boundary:&CompletionBoundary,proof:&NativeCompletionInput,
    limits:&RuntimeLimits)->bool {
    let Some(first)=native_target(target) else {return false;};
    let CompletionBoundary::NativeResponse {request_index:last}=boundary else {return false;};
    let Some(policy)=limits.governance.as_ref() else {return false;};
    let Ok(count)=u32::try_from(proof.rejections.len()) else {return false;};
    if proof.rejections.len()>policy.scan_rows || first.checked_add(count)!=Some(*last) {return false;}
    let mut ids=std::collections::BTreeSet::new();
    proof.rejections.iter().enumerate().all(|(offset,item)| {
        first.checked_add(offset as u32)==Some(item.request_index)
            && crate::mcp::valid_identity(&item.operation_id,limits.transport.identity_bytes)
            && ids.insert(item.operation_id.as_str())
    }) && bounded_json_size(proof,policy.record_bytes).is_ok()
}

fn validate_native_input(target:&CompletionTarget,proof:&NativeCompletionInput,index:u32,
    request_limit:u32,request_bytes:usize,limits:&RuntimeLimits)->Result<(),String> {
    proof.request_binding.validate(request_bytes)?;
    if index>request_limit || !native_consumes(target,&CompletionBoundary::NativeResponse {request_index:index},proof,limits) {
        return Err("completion native input lacks its bounded contiguous original rejection chain".into());
    }
    Ok(())
}

impl Host {
    pub(super) fn open(client: ApiClient, caller: Caller, session_id: &str, state: &Path) -> Result<Self, String> {
        caller.validate(&client.limits().transport)?;
        if !crate::mcp::valid_identity(session_id, client.limits().transport.identity_bytes) {
            return Err("completion session identity invalid".into());
        }
        let directory = state.join("completion").join(key(&caller)?);
        process::private_directory(&directory)?;
        Ok(Self {client,caller,session_id:session_id.into(),directory})
    }

    fn journal(&self, directory: &Path, name: &str, id: String,
        request: impl FnOnce(String) -> Operation) -> Result<(PathBuf,mutations::Journal),String> {
        process::private_directory(directory)?;
        let path = directory.join(name);
        let old = mutations::read(&path,self.client.limits())?;
        let request = request(id);
        let journal = match old {
            Some(journal) => {
                if serde_json::to_value(&journal.request).map_err(|e|e.to_string())? != serde_json::to_value(&request).map_err(|e|e.to_string())? {return Err("completion original request differs".into());}
                journal
            },
            None => mutations::create(&path,request,0,self.client.limits())?,
        };
        Ok((path,journal))
    }

    async fn resolve(&self, path: &Path, journal: &mut mutations::Journal, control: &Control) -> Result<Outcome,String> {
        control.check()?;
        let mut stopped = control.clone();
        let outcome = tokio::select! {biased;
            _=stopped.stopped()=>return Err("completion exchange interrupted; original operation retained".into()),
            outcome=mutations::resolve(&self.client,path,journal)=>outcome?,
        }.ok_or("completion exchange unresolved; original operation retained")?;
        control.check()?;
        Ok(outcome)
    }

    // The refusal receipt contains the decision identity, not its reason body.
    // Read that same immutable decision; current input authority remains the
    // controller's Begin check under the current caller.
    async fn blocked_evaluation(&self, journal: &mutations::Journal, id: DecisionRef,
        boundary: &CompletionBoundary, control: &Control) -> Result<CompletionEvaluation,String> {
        use crate::governance::{ActualDecisionOutcome, DecisionDetail, DecisionMode, GovernanceQuery, GovernanceResult};
        use crate::collaboration::workflow::{ActorProvenance, PrincipalRef};
        let request=Operation::QueryGovernance {query:GovernanceQuery::Decision {decision:id.clone()}};
        control.check()?;
        let mut stopped=control.clone();
        let reply=tokio::select! {biased;
            _=stopped.stopped()=>return Err("completion decision lookup interrupted".into()),
            reply=self.client.call(request.clone())=>reply?,
        };
        reply.validate_for(&request)?;
        let Outcome::Ok {data:Data::Governance(value)}=reply.outcome else {
            return Err("original completion decision is unavailable".into());
        };
        let GovernanceResult::Decision(record)=*value else {return Err("completion lookup returned another governance kind".into());};
        let original=journal.outcome_context.as_ref().ok_or("completion original commit absent")?;
        let actor_matches=matches!((&record.actor.principal,&record.actor.provenance),
            (PrincipalRef::Agent {agent_id:principal},ActorProvenance::BoundAgentSession {
                agent_id,session_id,task_id,attempt_id,generation,worker_id,incarnation})
            if principal==agent_id && session_id==&self.session_id && task_id==&self.caller.task_id
                && attempt_id==&self.caller.attempt_id && *generation==self.caller.generation
                && worker_id==&self.caller.worker_id && incarnation==&self.caller.incarnation);
        if record.id!=id || record.mode!=DecisionMode::Enforcing
            || record.actual_outcome!=ActualDecisionOutcome::GovernanceBlocked || !actor_matches
            || record.controller_revision!=original.commit.controller_revision {
            return Err("completion lookup differs from its original enforcing refusal".into());
        }
        let DecisionDetail::Completion(state)=record.detail else {return Err("completion decision kind differs".into());};
        let limits=self.client.limits().governance.as_ref().ok_or("completion policy absent")?;
        if !self.matches_subject(&state.subject) || !matches!(state.disposition,CompletionDisposition::Blocked {..})
            || state.reasons.len()>limits.reason_items || bounded_json_size(&state.reasons,limits.reason_bytes).is_err() {
            return Err("completion decision subject or reasons differ".into());
        }
        control.check()?;
        Ok(CompletionEvaluation {decision:id,state,boundary:boundary.clone()})
    }

    pub(super) async fn evaluate(&self, boundary: &CompletionBoundary,
        intervening_requests: &[crate::native_loop_contract::NativeRequestEvidence], control: &Control) -> Result<Gate,String> {
        if self.client.limits().governance.is_none() {return Ok(Gate::Admit);}
        let directory = self.directory.join(key(boundary)?);
        let (path,mut journal) = self.journal(&directory,"evaluate.json",evaluation_id(&self.caller,boundary).map_err(|e|e.to_string())?,|operation_id| {
            Operation::EvaluateCompletion(EvaluateCompletion {operation_id,caller:self.caller.clone(),boundary:boundary.clone(),intervening_requests:intervening_requests.to_vec()})
        })?;
        let outcome = self.resolve(&path,&mut journal,control).await?;
        let id = DecisionRef {operation_id:operation_id(&journal.request)?.into(),point:DecisionPoint::Completion};
        let gate = match outcome {
            Outcome::Ok {data:Data::Completion(value)} => match *value {
                CompletionOutcome::NotEnforced => Gate::Admit,
                CompletionOutcome::Evaluated(value) => {
                    if value.decision != id || value.boundary != *boundary
                        || !self.matches_subject(&value.state.subject)
                        || value.state.disposition != CompletionDisposition::Admit {
                        return Err("completion decision does not bind this admitted boundary".into());
                    }
                    Gate::Admit
                },
                _ => return Err("unexpected completion evaluation outcome".into()),
            },
            Outcome::Refused {code,..} if code == "governance_completion_blocked" => Gate::Blocked(self.blocked_evaluation(&journal,id,boundary,control).await?),
            Outcome::Refused {..} => Gate::Held,
            _ => return Err("unexpected completion evaluation outcome".into()),
        };
        mutations::consume(&path,&mut journal)?;
        Ok(gate)
    }

    fn matches_subject(&self, subject: &crate::governance::CompletionSubject) -> bool {
        subject.task_id == self.caller.task_id && subject.session_id == self.session_id
            && subject.attempt_id == self.caller.attempt_id && subject.generation == self.caller.generation
    }

    pub(super) async fn begin(&self, boundary: &CompletionBoundary, decision: &DecisionRef,
        context: &str, target: CompletionTarget, delivery_batch_id: Option<String>, control: &Control)
        -> Result<Option<Input>,String> {
        if !matches!(&target,CompletionTarget::NativeRequest {..} | CompletionTarget::CodexTurn {..}) {
            return Err("in-place completion target differs".into());
        }
        self.begin_inner(boundary,decision,context,target,delivery_batch_id,None,control).await
    }

    /// Only the current RunAttempt's exact immutable resume carrier selects
    /// this path. The original decision still describes its prior attempt.
    pub(super) async fn begin_initial(&self, resume:&CompletionResume,context:&str,
        target:CompletionTarget,delivery_batch_id:Option<String>,control:&Control) -> Result<Option<Input>,String> {
        let subject=&resume.evaluation.state.subject;
        if subject.task_id != self.caller.task_id || subject.session_id != self.session_id
            || subject.attempt_id == self.caller.attempt_id || subject.generation >= self.caller.generation
            || !matches!(&resume.evaluation.boundary,CompletionBoundary::StoppedResult {..})
            || !matches!(&target,CompletionTarget::InitialNativeRequest {request_index:1} | CompletionTarget::InitialCodexTurn {..}) {
            return Err("completion initial input does not bind the original stopped decision and successor".into());
        }
        self.begin_inner(&resume.evaluation.boundary,&resume.evaluation.decision,context,target,
            delivery_batch_id,Some(resume),control).await
    }

    async fn begin_inner(&self,boundary:&CompletionBoundary,decision:&DecisionRef,
        context:&str,target:CompletionTarget,delivery_batch_id:Option<String>,
        original:Option<&CompletionResume>,control:&Control) -> Result<Option<Input>,String> {
        let directory = self.directory.join(key(boundary)?);
        let (path,mut journal) = self.journal(&directory,"begin.json",begin_id(&self.caller,decision).map_err(|e|e.to_string())?,|operation_id| {
            Operation::BeginCompletionContinuation(BeginCompletionContinuation {operation_id,caller:self.caller.clone(),
                decision:decision.clone(),context_binding:context.into(),target:target.clone(),delivery_batch_id:delivery_batch_id.clone()})
        })?;
        let outcome = self.resolve(&path,&mut journal,control).await?;
        let feedback = match outcome {
            Outcome::Refused {..} => {mutations::consume(&path,&mut journal)?;return Ok(None);},
            Outcome::Ok {data:Data::Completion(value)} => match *value {
                CompletionOutcome::Begun(feedback) => feedback,
                _ => return Err("unexpected completion Begin outcome".into()),
            },
            _ => return Err("unexpected completion Begin outcome".into()),
        };
        let expected = CompletionContinuation {begin_operation_id:operation_id(&journal.request)?.into(),caller:self.caller.clone(),
            decision:decision.clone(),context_binding:context.into(),target,delivery_batch_id,acknowledged:None};
        if feedback.continuation != expected || feedback.evaluation.decision != *decision
            || feedback.evaluation.boundary != *boundary
            || match original {Some(original)=>feedback.evaluation != original.evaluation,None=>!self.matches_subject(&feedback.evaluation.state.subject)}
            || !matches!(feedback.evaluation.state.disposition,CompletionDisposition::Blocked {..})
            || !original.is_some_and(|resume|resume.reset_event.is_some())
                && !matches!(feedback.evaluation.state.disposition,CompletionDisposition::Blocked {continuation_eligible:true,exhausted:false}) {
            return Err("completion Begin does not bind this exact eligible decision".into());
        }
        let text = feedback_text(&feedback,self.client.limits())?;
        let record = InputRecord {revision:1,feedback,text,started:false,accepted_at:None,consumed_at:None,native_input:None,supplied_retries:Vec::new()};
        let input_path = directory.join("input.json");
        if let Some(old) = read_input(&input_path,self.client.limits())? {
            if old != record {return Err("completion input already dispatched or differs; no resend".into());}
        }
        persist_input(&input_path,&record,self.client.limits())?;
        mutations::consume(&path,&mut journal)?;
        Ok(Some(Input {directory,record}))
    }

    /// The producer calls this only after its exact completed response/turn.
    pub(super) async fn acknowledged(&self, input: &mut Input, boundary: CompletionBoundary,
        delivery_ack_operation_id: Option<String>, native_input: Option<NativeCompletionInput>, control: &Control)
        -> Result<CompletionContinuation,String> {
        if input.delivery_batch_id().is_some() != delivery_ack_operation_id.is_some()
            || !input.record.started || match &native_input {
                Some(proof)=>!native_consumes(&input.record.feedback.continuation.target,&boundary,proof,self.client.limits())
                    || match &input.record.native_input {Some(old)=>old != proof,None=>!proof.rejections.is_empty()},
                None=>matches!(&boundary,CompletionBoundary::NativeResponse {..})
                    || !consumes(&input.record.feedback.continuation.target,&boundary),
            }
            || matches!(&boundary,CompletionBoundary::CodexTurn {..}) && input.record.supplied_retries.last().or(input.record.accepted_at.as_ref()) != Some(&boundary)
            || input.record.consumed_at.as_ref().is_some_and(|old|old != &boundary) {
            return Err("completion input lacks its actual consuming boundary".into());
        }
        input.record.consumed_at = Some(boundary.clone());
        input.record.native_input = native_input.clone();
        persist_input(&input.directory.join("input.json"),&input.record,self.client.limits())?;
        let (path,mut journal) = self.journal(&input.directory,"acknowledge.json",acknowledgement_id(&self.caller,&input.record.feedback.continuation.begin_operation_id).map_err(|e|e.to_string())?,|operation_id| {
            Operation::AcknowledgeCompletionInput(AcknowledgeCompletionInput {operation_id,caller:self.caller.clone(),
                begin_operation_id:input.record.feedback.continuation.begin_operation_id.clone(),consumed_at:boundary.clone(),
                delivery_ack_operation_id:delivery_ack_operation_id.clone(),native_input:native_input.clone()})
        })?;
        let outcome = self.resolve(&path,&mut journal,control).await?;
        let mut expected = input.record.feedback.continuation.clone();
        expected.acknowledged = Some(CompletionInputAck {operation_id:operation_id(&journal.request)?.into(),consumed_at:boundary,delivery_ack_operation_id,native_input});
        match outcome {
            Outcome::Ok {data:Data::Completion(value)} => match *value {
                CompletionOutcome::InputAcknowledged(value) if value == expected => {
                    mutations::consume(&path,&mut journal)?;
                    Ok(value)
                },
                _ => Err("completion input acknowledgment unavailable; original intent retained".into()),
            },
            _ => Err("completion input acknowledgment unavailable; original intent retained".into()),
        }
    }
}

#[cfg(test)]
#[path = "completion_tests.rs"]
pub(super) mod tests;
