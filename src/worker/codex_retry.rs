//! Same-attempt supplied outer continuation. This journal is the before-send
//! fence; canonical worker receipts hold original facts and admission outcomes.
use super::*;
use crate::supplied_outer::{Cause, Observation as Outer, RetryPolicy};
use serde::{Deserialize, Serialize};
use std::io::Read;

pub(in crate::worker) const JOURNAL: &str = "borg-supplied-outer.json";
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(super) struct Record {
    revision: u32,
    cwd: PathBuf,
    home: PathBuf,
    cwd_identity: (u64, u64),
    home_identity: (u64, u64),
    binding: Value,
    deadline_ms: u64,
    policy: RetryPolicy,
    pub(super) index: u32,
    pub(super) thread_id: String,
    pub(super) turn_id: Option<String>,
    pub(super) phase: Option<Outer>,
    pub(super) committed: bool,
    pub(super) dispatched: bool,
    pub(super) terminal: bool,
    pub(super) retry_index: u32,
    wait: Option<crate::supplied_outer::Wait>,
    last_terminal: Option<Value>,
}
fn directory_identity(path: &Path) -> std::result::Result<(u64, u64), String> {
    use std::os::unix::fs::MetadataExt;
    let m =
        std::fs::symlink_metadata(path).map_err(|_| "supplied retained directory unavailable")?;
    if !m.is_dir()
        || m.uid()
            != std::fs::metadata("/proc/self")
                .map_err(|_| "owner unavailable")?
                .uid()
    {
        return Err("supplied retained directory ownership or type differs".into());
    }
    Ok((m.dev(), m.ino()))
}
pub(super) fn read(state: &Path, control: &Control) -> std::result::Result<Option<Record>, String> {
    read_at(state, control.deadline_ms, &control.worker_limits)
}
fn read_at(
    state: &Path,
    deadline_ms: u64,
    worker_limits: &WorkerLimits,
) -> std::result::Result<Option<Record>, String> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    let file = match std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(0x20000 | 0x800)
        .open(state.join(JOURNAL))
    {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err("supplied outer journal unavailable".into()),
    };
    let meta = file
        .metadata()
        .map_err(|_| "supplied journal metadata unavailable")?;
    if !meta.is_file()
        || meta.mode() & 0o077 != 0
        || meta.uid()
            != std::fs::metadata("/proc/self")
                .map_err(|_| "owner unavailable")?
                .uid()
    {
        return Err("supplied journal owner or type differs".into());
    }
    let mut bytes = Vec::new();
    file.take(worker_limits.mutation_record_bytes as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| "supplied journal read failed")?;
    if bytes.len() > worker_limits.mutation_record_bytes {
        return Err("supplied journal exceeds worker.mutation_record_bytes".into());
    }
    let record: Record =
        serde_json::from_slice(&bytes).map_err(|_| "supplied outer journal invalid")?;
    if record.revision != 1
        || record.index == 0
        || record.deadline_ms != deadline_ms
        || !record.cwd.is_absolute()
        || record.home != state
        || directory_identity(state)? != record.home_identity
    {
        return Err("supplied outer journal binding differs".into());
    }
    record.policy.validate()?;
    Ok(Some(record))
}
pub(super) struct Owner {
    state: PathBuf,
    record: Record,
}
impl Owner {
    pub(super) fn new(
        state: &Path,
        cwd: &Path,
        thread: &str,
        policy: &RetryPolicy,
        binding: Value,
        control: &Control,
    ) -> std::result::Result<Self, String> {
        if read(state, control)?.is_some() {
            return Err("retained supplied occurrence requires original reconciliation; no new thread or start".into());
        }
        let owner = Self {
            state: state.into(),
            record: Record {
                revision: 1,
                cwd: cwd.into(),
                home: state.into(),
                cwd_identity: directory_identity(cwd)?,
                home_identity: directory_identity(state)?,
                binding,
                deadline_ms: control.deadline_ms,
                policy: policy.clone(),
                index: 1,
                thread_id: thread.into(),
                turn_id: None,
                phase: None,
                committed: false,
                dispatched: false,
                terminal: false,
                retry_index: 0,
                wait: None,
                last_terminal: None,
            },
        };
        owner.save(control)?;
        Ok(owner)
    }
    fn save(&self, control: &Control) -> std::result::Result<(), String> {
        crate::protocol::bounded_json_size(
            &self.record,
            control.worker_limits.mutation_record_bytes,
        )
        .map_err(|_| "supplied journal exceeds mutation bound")?;
        process::atomic_json(&self.state.join(JOURNAL), &self.record)
    }
    pub(super) fn before_send(&mut self, control: &Control) -> std::result::Result<(), String> {
        control.check()?;
        if directory_identity(&self.record.cwd)? != self.record.cwd_identity {
            return Err("supplied workspace identity changed".into());
        }
        if self.record.dispatched
            || self.record.index > 1 && !matches!(self.record.phase, Some(Outer::Dispatch { .. }))
            || self.record.index > 1 && !self.record.committed
        {
            return Err(
                "supplied start already sent or admission unacknowledged; no resend".into(),
            );
        }
        self.record.dispatched = true;
        self.record.terminal = false;
        self.save(control)
    }
    async fn report(
        &mut self,
        o: Outer,
        control: &Control,
        events: &mpsc::Sender<Event>,
    ) -> std::result::Result<(), String> {
        self.record.phase = Some(o.clone());
        self.record.committed = false;
        self.save(control)?;
        let (acknowledged, received) = tokio::sync::oneshot::channel();
        events
            .send(Event::SuppliedOuter {
                observation: o,
                acknowledged,
            })
            .await
            .map_err(|_| "supplied observer closed")?;
        let mut stopped = control.clone();
        let wait = tokio::select! {biased; _=stopped.stopped()=>return Err("supplied original report interrupted; retained for reconciliation".into()), result=received=>result.map_err(|_|"supplied original receipt unavailable")??,};
        if let Some(wait) = wait {
            self.record.retry_index = wait.rate_failures;
            self.record.wait = Some(wait);
        }
        // Publish the acknowledged grant and its definitely-unsent successor
        // in one atomic journal replacement. No durable intermediate record may
        // confuse the predecessor's send marker with this new occurrence.
        if let Some(Outer::Dispatch { index, .. }) = self.record.phase {
            self.record.index = index;
            self.record.turn_id = None;
            self.record.dispatched = false;
            self.record.terminal = false;
        }
        self.record.committed = true;
        self.save(control)?;
        if matches!(self.record.phase, Some(Outer::Waiting { .. })) {
            let wait = self
                .record
                .wait
                .as_ref()
                .ok_or("original supplied wait absent")?;
            let policy = &self.record.policy;
            events.send(Event::Progress(format!("Supplied outer {} waiting: thread={} prior_turn={} rate_failures={} admitted_ms={} due_ms={} delay_ms={} policy_initial_ms={} multiplier={} maximum_ms={} jitter_ms={}",
                wait.index,self.record.thread_id,self.record.turn_id.as_deref().unwrap_or("unavailable"),wait.rate_failures,
                wait.admitted_at_ms,wait.not_before_ms,wait.not_before_ms.saturating_sub(wait.admitted_at_ms),policy.initial_backoff_ms,
                policy.backoff_multiplier,policy.max_backoff_ms,policy.jitter_ms))).await.map_err(|_|"supplied progress observer closed")?;
        }
        Ok(())
    }
    pub(super) async fn acknowledged(
        &mut self,
        turn: &str,
        control: &Control,
        events: &mpsc::Sender<Event>,
    ) -> std::result::Result<(), String> {
        if !self.record.dispatched || self.record.turn_id.is_some() {
            return Err("supplied ACK differs from original dispatch".into());
        }
        self.record.turn_id = Some(turn.into());
        self.save(control)?;
        self.report(
            Outer::Acknowledged {
                index: self.record.index,
                thread_id: self.record.thread_id.clone(),
                turn_id: turn.into(),
            },
            control,
            events,
        )
        .await
    }
    pub(super) async fn terminal(
        &mut self,
        message: &Value,
        control: &Control,
        events: &mpsc::Sender<Event>,
        credentials: &[String],
    ) -> std::result::Result<(), String> {
        let t = &message["params"]["turn"];
        if message["method"] != "turn/completed"
            || message["params"]["threadId"] != self.record.thread_id
            || t["id"].as_str() != self.record.turn_id.as_deref()
        {
            return Err("supplied terminal differs from acknowledged occurrence".into());
        }
        let terminal = json!({"id":t["id"],"status":t["status"],"error":t.get("error").cloned().unwrap_or(Value::Null)});
        if super::super::value_contains_credential(&terminal, credentials, &control.worker_limits)?
        {
            return Err("supplied terminal contains configured credential; withheld".into());
        }
        self.record.last_terminal = Some(terminal.clone());
        self.report(
            Outer::Terminal {
                index: self.record.index,
                thread_id: self.record.thread_id.clone(),
                turn_id: self.record.turn_id.clone().ok_or("supplied ACK absent")?,
                terminal,
            },
            control,
            events,
        )
        .await?;
        self.record.terminal = true;
        self.save(control)
    }
    pub(super) async fn reserve(
        &mut self,
        cause: Cause,
        control: &Control,
        events: &mpsc::Sender<Event>,
    ) -> std::result::Result<u32, String> {
        if !self.record.terminal || !self.record.committed {
            return Err("supplied terminal requires original reconciliation".into());
        }
        let index = self
            .record
            .index
            .checked_add(1)
            .ok_or("supplied outer index exhausted")?;
        let previous_turn_id = self
            .record
            .turn_id
            .clone()
            .ok_or("supplied prior turn absent")?;
        self.report(
            Outer::Waiting {
                index,
                thread_id: self.record.thread_id.clone(),
                previous_turn_id: previous_turn_id.clone(),
                cause: cause.clone(),
            },
            control,
            events,
        )
        .await?;
        if let Cause::RateLimit { .. } = cause {
            let retry_at_ms = self
                .record
                .wait
                .as_ref()
                .filter(|w| w.index == index)
                .ok_or("canonical retry wait absent")?
                .not_before_ms;
            let mut stopped = control.clone();
            tokio::select! {biased; _=stopped.stopped()=>return Err("supplied retry cancelled or deadline reached".into()),_=tokio::time::sleep(Duration::from_millis(retry_at_ms.saturating_sub(process::now_ms())))=>{}}
        }
        control.check()?;
        self.report(
            Outer::Dispatch {
                index,
                thread_id: self.record.thread_id.clone(),
                previous_turn_id,
                cause,
            },
            control,
            events,
        )
        .await?;
        Ok(index - 1)
    }
    pub(super) fn rate_cause(&self, _control: &Control) -> std::result::Result<Cause, String> {
        Ok(Cause::RateLimit {
            retry_index: self
                .record
                .retry_index
                .checked_add(1)
                .ok_or("supplied retry index exhausted")?,
        })
    }
    pub(super) fn index(&self) -> u32 {
        self.record.index
    }

    /// A cold app-server is permitted only for a retained, acknowledged
    /// terminal followed by an admitted but definitely unsent new occurrence.
    /// The surrounding live worker retains its original canonical ownership.
    pub(super) fn resume_predecessor(
        &self,
        control: &Control,
    ) -> std::result::Result<(&str, &str), String> {
        control.check()?;
        if read(&self.state, control)?.as_ref() != Some(&self.record)
            || self.record.dispatched
            || !self.record.committed
        {
            return Err("supplied resume lacks exact known-unsent current occurrence".into());
        }
        match self.record.phase.as_ref() {
            Some(Outer::Dispatch{thread_id,previous_turn_id,cause:Cause::RateLimit{..},..})=>Ok((thread_id,previous_turn_id)),
            _=>Err("supplied resume requires an acknowledged rate terminal and original dispatch admission".into()),
        }
    }
    pub(super) fn verify_history(
        &self,
        thread: &Value,
        cwd: &Path,
        control: &Control,
    ) -> std::result::Result<(), String> {
        let (id, turn) = self.resume_predecessor(control)?;
        if thread["id"] != id
            || thread["cwd"].as_str() != cwd.to_str()
            || directory_identity(cwd)? != self.record.cwd_identity
        {
            return Err("retained supplied thread or workspace differs".into());
        }
        let turns = thread["turns"]
            .as_array()
            .ok_or("retained supplied turns unavailable")?;
        let mut selected = turns.iter().filter(|t| t["id"] == turn);
        let actual = selected
            .next()
            .ok_or("original supplied terminal unavailable")?;
        let projected = json!({"id":actual["id"],"status":actual["status"],"error":actual.get("error").cloned().unwrap_or(Value::Null)});
        if selected.next().is_some()
            || self.record.last_terminal.as_ref() != Some(&projected)
            || !crate::supplied_outer::rate_limited(&projected)
            || turns.iter().any(|t| t["status"] == "inProgress")
        {
            return Err("retained supplied history differs from exact original terminal".into());
        }
        Ok(())
    }
}

// Called only by existing terminal, stopped, no-outbox cleanup admission. A
// known terminal followed by a cancelled wait is disposable under its selected
// grace; an unknown start or unacknowledged original fact is still an obligation.
fn preadvance_unsent(r: &Record) -> bool {
    matches!(&r.phase,Some(Outer::Dispatch{index,previous_turn_id,..})
        if r.index.checked_add(1)==Some(*index)&&r.terminal
            &&r.turn_id.as_deref()==Some(previous_turn_id.as_str()))
}
pub(in crate::worker) fn cleanup_ready(
    state: &Path,
    cwd: &Path,
    deadline_ms: u64,
    limits: &WorkerLimits,
) -> std::result::Result<bool, String> {
    let Some(r) = read_at(state, deadline_ms, limits)? else {
        return Ok(true);
    };
    if r.cwd != cwd {
        return Err("cleanup supplied workspace differs".into());
    }
    Ok(r.last_terminal.is_some()
        && (matches!(
            r.phase,
            Some(Outer::Terminal { .. } | Outer::Waiting { .. })
        ) || preadvance_unsent(&r)
            || (r.committed
                && !r.dispatched
                && r.turn_id.is_none()
                && matches!(r.phase,Some(Outer::Dispatch{index,..}) if index==r.index))))
}

pub(in crate::worker) async fn cleanup_verified(
    client: &crate::client::ApiClient,
    state: &Path,
    input: &crate::protocol::TaskInput,
    attempt: &crate::protocol::Attempt,
    timeout_ms: u64,
) -> std::result::Result<bool, String> {
    use crate::protocol::{Data, Observation, Operation, Outcome, Report};
    let Some(r) = read_at(
        state,
        attempt.deadline_unix_ms.min(input.limits.deadline_unix_ms),
        &client.limits().worker,
    )?
    else {
        return Ok(true);
    };
    let caller = super::super::collaboration_caller(attempt);
    if r.binding["caller"]
        != serde_json::to_value(&caller).map_err(|_| "supplied owner encoding failed")?
        || r.binding["session_id"] != attempt.session_id
        || input.execution_limits()?.coding.rate_limit_retry.as_ref() != Some(&r.policy)
    {
        return Err("supplied cleanup differs from accepted owner or retry policy".into());
    }
    let preadvance = preadvance_unsent(&r);
    let Some(terminal) = r.last_terminal else {
        return Ok(false);
    };
    let (index, turn_id) = match &r.phase {
        Some(Outer::Terminal { .. } | Outer::Waiting { .. }) => {
            let Some(turn) = r.turn_id else {
                return Ok(false);
            };
            (r.index, turn)
        }
        Some(Outer::Dispatch {
            index,
            previous_turn_id,
            ..
        }) if preadvance
            || (r.committed && !r.dispatched && r.turn_id.is_none() && *index == r.index) =>
        {
            // The canonical dispatch is a debit/admission, not evidence of send.
            // Confirm its exact original receipt as well as the predecessor terminal.
            let id = crate::supplied_outer::id(&attempt.id, *index, "dispatch");
            let intended = Operation::ObserveWorker {
                report: Report {
                    report_id: id.clone(),
                    worker_id: attempt.worker_id.clone(),
                    incarnation: attempt.worker_incarnation.clone(),
                    task_id: attempt.task_id.clone(),
                    attempt_id: attempt.id.clone(),
                    generation: attempt.generation,
                    observation: Observation::SuppliedOuter {
                        observation: r.phase.clone().unwrap(),
                    },
                },
            };
            if !cleanup_original(client, &id, &intended, attempt, timeout_ms, preadvance).await? {
                return Ok(false);
            }
            (
                index
                    .checked_sub(1)
                    .ok_or("supplied cleanup index invalid")?,
                previous_turn_id.clone(),
            )
        }
        _ => return Ok(false),
    };
    let id = crate::supplied_outer::id(&attempt.id, index, "terminal");
    let expected = Operation::ObserveWorker {
        report: Report {
            report_id: id.clone(),
            worker_id: attempt.worker_id.clone(),
            incarnation: attempt.worker_incarnation.clone(),
            task_id: attempt.task_id.clone(),
            attempt_id: attempt.id.clone(),
            generation: attempt.generation,
            observation: Observation::SuppliedOuter {
                observation: Outer::Terminal {
                    index,
                    thread_id: r.thread_id,
                    turn_id,
                    terminal,
                },
            },
        },
    };
    cleanup_original(client, &id, &expected, attempt, timeout_ms, false).await
}
async fn cleanup_original(
    client: &crate::client::ApiClient,
    id: &str,
    expected: &crate::protocol::Operation,
    attempt: &crate::protocol::Attempt,
    timeout_ms: u64,
    allow_refusal: bool,
) -> std::result::Result<bool, String> {
    use crate::protocol::{Data, Operation, Outcome};
    let reply = tokio::time::timeout(
        Duration::from_millis(timeout_ms),
        client.call(Operation::GetOperation {
            operation_id: id.to_owned(),
        }),
    )
    .await
    .map_err(|_| "supplied cleanup original lookup timed out")??;
    let Outcome::Ok {
        data: Data::Operation(record),
    } = reply.outcome
    else {
        return Ok(false);
    };
    if record.id != id
        || record.state != "settled"
        || record.intended
            != serde_json::to_value(expected)
                .map_err(|_| "supplied cleanup original encoding failed")?
    {
        return Ok(false);
    }
    let original: Outcome = serde_json::from_value(
        record
            .outcome
            .ok_or("supplied terminal original outcome absent")?,
    )
    .map_err(|_| "supplied terminal original outcome invalid")?;
    Ok(
        matches!(&original,Outcome::Ok{data:Data::Task(task)} if task.id==attempt.task_id&&task.attempt.as_ref().is_some_and(|a|a.id==attempt.id&&a.generation==attempt.generation))
            || (allow_refusal && matches!(original, Outcome::Refused { .. })),
    )
}

#[cfg(test)]
#[path = "codex_retry_tests.rs"]
mod tests;
