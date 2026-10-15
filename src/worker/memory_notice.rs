//! Bounded host delivery of controller-owned memory invalidation batches.
//! An append/submission acknowledgment is never evidence of model consumption.
use super::*;
use crate::collaboration::{
    AgentMemoryBatch, AgentMemoryCursor, AgentMemoryObservation, AgentMemoryPage,
};
use crate::limits::MemoryNoticeLimits;
use crate::mcp::Caller;
use std::io::Read;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Notice {
    pub batch_id: String,
    pub text: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AppendDisposition {
    Ready,
    /// Preserve pending delivery and continue only at a later safe boundary.
    /// This is not permission to retransmit or request a notice-only model turn.
    Deferred,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Delivery {
    caller: Caller,
    context: String,
    appended: bool,
}
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Pending {
    batch: AgentMemoryBatch,
    text: String,
    delivery: Option<Delivery>,
}
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Mirror {
    limits: MemoryNoticeLimits,
    revision: u32,
    start_sequence: u64,
    acknowledged_cursor: AgentMemoryCursor,
    pending: Option<Pending>,
}

/// One host at a time, under the owning worker/attempt's existing lease.
/// The session-scoped file is only a delivery mirror. Controller state is the
/// authority when an admitted replacement executes on another worker.
pub(super) struct Host {
    limits: MemoryNoticeLimits,
    client: ApiClient,
    caller: Caller,
    session_id: String,
    agent_id: String,
    directory: PathBuf,
    credentials: Vec<String>,
    mirror: Option<Mirror>,
    current_confirmed: bool,
}

impl Host {
    pub(super) fn open(
        client: ApiClient,
        state: &Path,
        caller: &Caller,
        session_id: &str,
        agent_id: &str,
        limits: &MemoryNoticeLimits,
    ) -> Result<Self, String> {
        let credentials = configured_credentials(client.limits())?;
        Self::open_with_credentials(
            client,
            state,
            caller,
            session_id,
            agent_id,
            limits,
            credentials,
        )
    }

    fn open_with_credentials(
        client: ApiClient,
        state: &Path,
        caller: &Caller,
        session_id: &str,
        agent_id: &str,
        limits: &MemoryNoticeLimits,
        credentials: Vec<String>,
    ) -> Result<Self, String> {
        caller.validate(&client.limits().transport)?;
        limits.validate()?;
        if !crate::mcp::valid_identity(session_id, client.limits().transport.identity_bytes)
            || !crate::mcp::valid_identity(agent_id, client.limits().transport.identity_bytes)
        {
            return Err("invalid host memory scope".into());
        }
        let mut host = Self {
            limits: limits.clone(),
            client,
            caller: caller.clone(),
            session_id: session_id.into(),
            agent_id: agent_id.into(),
            directory: scope_directory(state, session_id),
            credentials,
            mirror: None,
            current_confirmed: false,
        };
        host.safe(&json!({"caller":caller,"session_id":session_id,"agent_id":agent_id}))?;
        let path = host.directory.join("delivery.json");
        if let Some(bytes) = read_private(
            &path,
            limits.mirror_bytes,
            "execution.memory_notice.mirror_bytes",
        )? {
            let mirror: Mirror =
                serde_json::from_slice(&bytes).map_err(|_| "invalid memory delivery mirror")?;
            host.validate_mirror(&mirror)?;
            host.mirror = Some(mirror);
        }
        Ok(host)
    }

    fn safe(&self, value: &impl Serialize) -> Result<(), String> {
        bounded_json_size(value, self.client.limits().transport.body_bytes)
            .map_err(|_| "memory notification data exceeds bound (transport.body_bytes)")?;
        let value =
            serde_json::to_value(value).map_err(|_| "memory notification encoding failed")?;
        if value_contains_credential(&value, &self.credentials, &self.client.limits().worker)
            .map_err(|reason| {
                format!("memory notification credential inspection exceeds bound: {reason}")
            })?
        {
            return Err("memory notification contains a configured credential".into());
        }
        Ok(())
    }

    fn cursor(&self, cursor: &AgentMemoryCursor) -> Result<(), String> {
        if !crate::mcp::valid_identity(
            &cursor.database_id,
            self.client.limits().transport.identity_bytes,
        ) || cursor.session_id != self.session_id
            || cursor.agent_id != self.agent_id
            || cursor.sequence > i64::MAX as u64
        {
            return Err("memory notification scope differs".into());
        }
        if let Some(mirror) = &self.mirror {
            if cursor.database_id != mirror.acknowledged_cursor.database_id {
                return Err("memory notification database differs".into());
            }
        }
        Ok(())
    }

    fn page(&self, page: &AgentMemoryPage) -> Result<(), String> {
        self.cursor(&page.from_cursor)?;
        self.cursor(&page.to_cursor)?;
        if page.from_cursor.database_id != page.to_cursor.database_id
            || page.to_cursor.sequence <= page.from_cursor.sequence
            || page.to_cursor.sequence > page.through_sequence
            || page.through_sequence > i64::MAX as u64
            || page.has_more != (page.to_cursor.sequence < page.through_sequence)
        {
            return Err("invalid bounded memory notification page".into());
        }
        if page.notices.len() > self.limits.keys {
            return Err(
                "invalid bounded memory notification page (execution.memory_notice.keys)".into(),
            );
        }
        let mut keys = BTreeSet::new();
        for notice in &page.notices {
            if !crate::mcp::valid_identity(
                &notice.key,
                self.client.limits().transport.identity_bytes,
            ) || notice.revision == 0
                || notice.revision > i64::MAX as u64
                || !keys.insert(&notice.key)
            {
                return Err("invalid memory notification hint".into());
            }
        }
        self.safe(page)
    }

    fn encode(&self, batch: &AgentMemoryBatch) -> Result<String, String> {
        self.page(&batch.page)?;
        if !crate::mcp::valid_identity(
            &batch.batch_id,
            self.client.limits().transport.identity_bytes,
        ) {
            return Err("invalid memory notification batch identity".into());
        }
        let text = serde_json::to_string(&json!({
            "type":"agent_memory_hints",
            "batch_id":batch.batch_id,
            "instruction":"Other sessions changed these shared agent memory keys. These are invalidation hints, not memory values. Call collab_memory_get for the current canonical value before relying on cached content.",
            "notices":batch.page.notices,
        })).map_err(|_| "memory notification encoding failed")?;
        // Bound its escaped representation as well as the model-visible bytes.
        bounded_json_size(&text, self.limits.notice_bytes).map_err(
            |_| "encoded memory notification exceeds bound (execution.memory_notice.notice_bytes)",
        )?;
        self.safe(&text)?;
        Ok(text)
    }

    fn validate_observation(&self, observation: &AgentMemoryObservation) -> Result<(), String> {
        self.safe(observation)?;
        self.cursor(&observation.acknowledged_cursor)?;
        if observation.start_sequence > observation.acknowledged_cursor.sequence
            || observation.pending.is_some() && observation.available.is_some()
        {
            return Err("invalid memory notification observation".into());
        }
        if let Some(mirror) = &self.mirror {
            if observation.start_sequence != mirror.start_sequence
                || observation.acknowledged_cursor.sequence < mirror.acknowledged_cursor.sequence
            {
                return Err("memory notification cursor or start changed".into());
            }
        }
        let page = if let Some(batch) = &observation.pending {
            self.encode(batch)?;
            Some(&batch.page)
        } else {
            observation.available.as_ref()
        };
        if let Some(page) = page {
            self.page(page)?;
            if page.from_cursor != observation.acknowledged_cursor {
                return Err(
                    "memory notification page does not begin at acknowledged cursor".into(),
                );
            }
        }
        Ok(())
    }

    fn validate_mirror(&self, mirror: &Mirror) -> Result<(), String> {
        if mirror.limits != self.limits
            || mirror.revision != 1
            || mirror.start_sequence > mirror.acknowledged_cursor.sequence
        {
            return Err("unsupported memory delivery mirror".into());
        }
        self.cursor(&mirror.acknowledged_cursor)?;
        if let Some(pending) = &mirror.pending {
            if pending.batch.page.from_cursor != mirror.acknowledged_cursor
                || pending.text != self.encode(&pending.batch)?
            {
                return Err("memory delivery mirror differs from exact batch".into());
            }
            if let Some(delivery) = &pending.delivery {
                delivery.caller.validate(&self.client.limits().transport)?;
                if delivery.caller.task_id != self.caller.task_id
                    || !crate::mcp::valid_identity(
                        &delivery.context,
                        self.client.limits().transport.identity_bytes,
                    )
                {
                    return Err("memory delivery context differs".into());
                }
            }
        }
        self.safe(mirror)
    }

    fn persist(&mut self, mirror: Mirror) -> Result<(), String> {
        self.validate_mirror(&mirror)?;
        bounded_json_size(&mirror, self.limits.mirror_bytes).map_err(
            |_| "memory delivery mirror exceeds bound (execution.memory_notice.mirror_bytes)",
        )?;
        process::private_directory(&self.directory)?;
        atomic_json(&self.directory.join("delivery.json"), &mirror)?;
        self.mirror = Some(mirror);
        Ok(())
    }

    fn accept(&mut self, observation: &AgentMemoryObservation) -> Result<(), String> {
        self.validate_observation(observation)?;
        let prior = self
            .mirror
            .as_ref()
            .and_then(|mirror| mirror.pending.as_ref());
        if let Some(prior) = prior {
            if observation.pending.as_ref() != Some(&prior.batch)
                && observation.acknowledged_cursor.sequence < prior.batch.page.to_cursor.sequence
            {
                return Err(
                    "controller memory pending batch changed without acknowledgment".into(),
                );
            }
        }
        let pending = observation
            .pending
            .as_ref()
            .map(|batch| -> Result<Pending, String> {
                if let Some(prior) = prior.filter(|pending| pending.batch == *batch) {
                    return Ok(prior.clone());
                }
                Ok(Pending {
                    batch: batch.clone(),
                    text: self.encode(batch)?,
                    delivery: None,
                })
            })
            .transpose()?;
        let mirror = Mirror {
            limits: self.limits.clone(),
            revision: 1,
            start_sequence: observation.start_sequence,
            acknowledged_cursor: observation.acknowledged_cursor.clone(),
            pending,
        };
        if self.mirror.as_ref() == Some(&mirror) {
            return Ok(());
        }
        // An empty first observation performs no local or controller mutation.
        if self.mirror.is_none() && mirror.pending.is_none() {
            self.mirror = Some(mirror);
            return Ok(());
        }
        self.persist(mirror)
    }

    async fn observe(&mut self, control: &Control) -> Result<AgentMemoryObservation, String> {
        control.check()?;
        let request = Operation::ObserveAgentMemory {
            caller: self.caller.clone(),
            limit: self.limits.keys,
        };
        let mut stopped = control.clone();
        let reply = tokio::select! { biased;
            _ = stopped.stopped() => return Err("memory observation interrupted".into()),
            reply = self.client.call(request.clone()) => reply?,
        };
        reply.validate_for(&request)?;
        let Outcome::Ok {
            data: Data::AgentMemory(observation),
        } = reply.outcome
        else {
            return Err("memory observation refused or unavailable".into());
        };
        self.accept(&observation)?;
        self.current_confirmed = true;
        Ok(observation)
    }

    fn mutation(
        &self,
        name: &str,
        request: Operation,
    ) -> Result<(PathBuf, mutations::Journal), String> {
        self.safe(&request)?;
        let path = self.directory.join(name);
        if let Some(journal) = self.read_mutation(&path)? {
            self.safe(&journal)?;
            if !journal.consumed {
                return Ok((path, journal));
            }
        }
        process::private_directory(&self.directory)?;
        let journal = mutations::create(&path, request, 0, self.client.limits())?;
        Ok((path, journal))
    }

    fn read_mutation(&self, path: &Path) -> Result<Option<mutations::Journal>, String> {
        let Some(bytes) = read_private(
            path,
            self.client.limits().worker.mutation_record_bytes,
            "worker.mutation_record_bytes",
        )?
        else {
            return Ok(None);
        };
        let journal: mutations::Journal =
            serde_json::from_slice(&bytes).map_err(|_| "invalid memory mutation journal")?;
        let original: serde_json::Value =
            serde_json::from_slice(&bytes).map_err(|_| "invalid memory mutation journal")?;
        self.safe(&journal)?;
        let id = match &journal.request {
            Operation::PrepareAgentMemory { operation_id, .. }
            | Operation::AcknowledgeAgentMemory { operation_id, .. } => operation_id,
            _ => return Err("unexpected memory mutation journal operation".into()),
        };
        if journal.limits != *self.client.limits()
            || original["revision"] != json!(3)
            || !crate::mcp::valid_identity(id, self.client.limits().transport.identity_bytes)
            || original.get("request")
                != Some(
                    &serde_json::to_value(&journal.request)
                        .map_err(|_| "memory mutation encoding failed")?,
                )
            || journal.consumed && journal.outcome.is_none()
            || journal.outcome.is_some() != journal.outcome_context.is_some()
        {
            return Err("unsupported memory mutation journal".into());
        }
        if let (Some(outcome), Some(context)) = (&journal.outcome, &journal.outcome_context) {
            if !matches!(outcome, Outcome::Ok { .. } | Outcome::Refused { .. }) {
                return Err("invalid memory mutation outcome".into());
            }
            context.validate(&journal.request, outcome)?;
        }
        Ok(Some(journal))
    }

    async fn resolve(
        &self,
        path: &Path,
        journal: &mut mutations::Journal,
        control: &Control,
    ) -> Result<Outcome, String> {
        self.safe(journal)?;
        if let Some(outcome) = &journal.outcome {
            mutations::persist(path, journal)?;
            return Ok(outcome.clone());
        }
        let id = match &journal.request {
            Operation::PrepareAgentMemory { operation_id, .. }
            | Operation::AcknowledgeAgentMemory { operation_id, .. } => operation_id,
            _ => return Err("unexpected memory notification mutation".into()),
        };
        control.check()?;
        let mut stopped = control.clone();
        let result: Result<Option<(Outcome, CommitContext)>, String> = tokio::select! { biased;
            _ = stopped.stopped() => return Err("memory mutation interrupted; exact outcome unknown".into()),
            value = async {
                match acceptance::lookup(&self.client, id, &journal.request).await? {
                    acceptance::Lookup::Recorded(outcome, context) => Ok(Some((outcome, context))),
                    acceptance::Lookup::Unavailable => Ok(None),
                    acceptance::Lookup::Absent => {
                        mutations::persist(path, journal)?;
                        control.check()?;
                        let reply = self.client.call(journal.request.clone()).await?;
                        Ok(CommitContext::from_reply(&reply, &journal.request)?
                            .map(|context| (reply.outcome, context)))
                    }
                }
            } => value,
        };
        let Some((outcome, context)) = result? else {
            return Err("memory mutation outcome unknown; exact request retained".into());
        };
        self.safe(&outcome)?;
        self.safe(&context)?;
        context.validate(&journal.request, &outcome)?;
        journal.outcome = Some(outcome.clone());
        journal.outcome_context = Some(context);
        atomic_json(path, journal)?;
        Ok(outcome)
    }

    async fn recover_prepare(&mut self, control: &Control) -> Result<(), String> {
        let path = self.directory.join("prepare.json");
        let Some(mut journal) = self.read_mutation(&path)? else {
            return Ok(());
        };
        if journal.consumed {
            return Ok(());
        }
        let Operation::PrepareAgentMemory {
            caller,
            expected_cursor,
            through_sequence,
            limit,
            ..
        } = &journal.request
        else {
            return Err("unexpected retained memory prepare".into());
        };
        caller.validate(&self.client.limits().transport)?;
        self.cursor(expected_cursor)?;
        if caller.task_id != self.caller.task_id
            || *limit == 0
            || *limit > self.limits.keys
            || *through_sequence <= expected_cursor.sequence
            || *through_sequence > i64::MAX as u64
        {
            return Err("retained memory prepare binding differs".into());
        }
        let previous_caller = caller != &self.caller;
        let expected_cursor = expected_cursor.clone();
        match self.resolve(&path, &mut journal, control).await? {
            Outcome::Ok {
                data: Data::AgentMemory(observation),
            } => self.accept_prepared(&observation, &expected_cursor)?,
            Outcome::Refused { .. } => {
                mutations::consume(&path, &mut journal)?;
                if previous_caller {
                    return Ok(());
                }
                return Err("memory prepare was refused; exact receipt retained".into());
            }
            _ => return Err("unexpected committed memory prepare response".into()),
        }
        mutations::consume(&path, &mut journal)
    }

    fn accept_prepared(
        &mut self,
        observation: &AgentMemoryObservation,
        expected: &AgentMemoryCursor,
    ) -> Result<(), String> {
        if observation.acknowledged_cursor != *expected || observation.pending.is_none() {
            return Err("memory prepare did not retain a batch at its exact cursor".into());
        }
        self.accept(observation)
    }

    // Covers ACK outcome -> mirror -> consumed crash windows, including another
    // admitted worker acknowledging the batch. Resolve the old exact request;
    // never overwrite its unknown outcome to make room for a new batch.
    async fn retire_ack(&mut self, control: &Control) -> Result<(), String> {
        let path = self.directory.join("acknowledge.json");
        let Some(mut journal) = self.read_mutation(&path)? else {
            return Ok(());
        };
        if journal.consumed {
            return Ok(());
        }
        let Operation::AcknowledgeAgentMemory {
            caller, batch_id, ..
        } = &journal.request
        else {
            return Err("unexpected retained memory acknowledgment".into());
        };
        caller.validate(&self.client.limits().transport)?;
        if caller.task_id != self.caller.task_id {
            return Err("retained memory acknowledgment task differs".into());
        }
        if self
            .mirror
            .as_ref()
            .and_then(|m| m.pending.as_ref())
            .is_some_and(|p| &p.batch.batch_id == batch_id)
        {
            return Ok(());
        }
        match self.resolve(&path, &mut journal, control).await? {
            Outcome::Ok {
                data: Data::AgentMemory(observation),
            } => {
                self.cursor(&observation.acknowledged_cursor)?;
                let mirror = self
                    .mirror
                    .as_ref()
                    .ok_or("memory acknowledgment scope absent")?;
                if observation.start_sequence != mirror.start_sequence
                    || observation.acknowledged_cursor.sequence
                        > mirror.acknowledged_cursor.sequence
                    || observation.pending.is_some()
                {
                    return Err("retained memory acknowledgment differs from current cursor".into());
                }
            }
            Outcome::Refused { .. } => {}
            _ => return Err("unexpected committed memory acknowledgment response".into()),
        }
        mutations::consume(&path, &mut journal)
    }

    /// Poll only at an already-required executor boundary. Empty history produces
    /// no mutation; unrelated history advances only within the accepted page budget.
    /// Exhausting that budget is not proof that the next boundary has no notice.
    pub(super) async fn prepare(&mut self, control: &Control) -> Result<Option<Notice>, String> {
        self.current_confirmed = false;
        self.recover_prepare(control).await?;
        let mut acknowledged_range_drained = false;
        for _ in 0..self.limits.empty_prefix_pages {
            let observation = self.observe(control).await?;
            self.retire_ack(control).await?;
            if observation.pending.is_none() {
                let Some(page) = observation.available else {
                    return Ok(None);
                };
                let expected_cursor = page.from_cursor.clone();
                // Account for encoded notice bytes and the controller's exact
                // batch-ID representation before retaining a pending batch.
                let limit = self.fitting_limit(&page)?;
                let request = Operation::PrepareAgentMemory {
                    operation_id: mutations::fresh_id("memory-prepare")?,
                    caller: self.caller.clone(),
                    expected_cursor: page.from_cursor,
                    through_sequence: page.through_sequence,
                    limit,
                };
                let (path, mut journal) = self.mutation("prepare.json", request)?;
                // No second logical prepare can replace an unresolved predecessor.
                if !matches!(&journal.request, Operation::PrepareAgentMemory { caller, .. } if caller == &self.caller)
                {
                    return Err("memory prepare predecessor awaits reconciliation".into());
                }
                match self.resolve(&path, &mut journal, control).await? {
                    Outcome::Ok {
                        data: Data::AgentMemory(observation),
                    } => self.accept_prepared(&observation, &expected_cursor)?,
                    Outcome::Refused { .. } => {
                        mutations::consume(&path, &mut journal)?;
                        return Err("memory prepare refused; no delivery begun".into());
                    }
                    _ => return Err("unexpected committed memory prepare response".into()),
                }
                mutations::consume(&path, &mut journal)?;
            }
            let pending = self
                .mirror
                .as_ref()
                .and_then(|m| m.pending.as_ref())
                .ok_or("memory prepare did not retain a batch")?;
            if pending.batch.page.notices.is_empty()
                || pending
                    .delivery
                    .as_ref()
                    .is_some_and(|d| d.appended && d.caller == self.caller)
            {
                // Only the acknowledged page's exact sequence range proves
                // drainage. Preserve the original cursor/ACK protocol even when
                // the configured budget cannot reach the next notice.
                acknowledged_range_drained =
                    pending.batch.page.to_cursor.sequence == pending.batch.page.through_sequence;
                self.acknowledge(control).await?;
                continue;
            }
            return Ok(Some(Notice {
                batch_id: pending.batch.batch_id.clone(),
                text: pending.text.clone(),
            }));
        }
        control.check()?;
        if acknowledged_range_drained {
            Ok(None)
        } else {
            Err("memory notification scan budget exhausted (execution.memory_notice.empty_prefix_pages)".into())
        }
    }

    fn fitting_limit(&self, page: &AgentMemoryPage) -> Result<usize, String> {
        if page.notices.is_empty() {
            return Ok(self.limits.keys);
        }
        let mut candidate = AgentMemoryBatch {
            // Controller allocation reads a nonnegative SQLite i64 counter and
            // refuses overflow. Its padded representation has this exact width
            // for every admitted sequence, without a policy-sized placeholder.
            batch_id: AgentMemoryBatch::identifier(&page.from_cursor.database_id, i64::MAX as u64),
            page: page.clone(),
        };
        for limit in (1..=page.notices.len()).rev() {
            candidate.page.notices.truncate(limit);
            if self.encode(&candidate).is_ok() {
                return Ok(limit);
            }
        }
        Err("memory notification cannot fit the delivery bound (execution.memory_notice.notice_bytes)".into())
    }

    /// Persist uncertainty before touching executor input. A new caller is
    /// admitted by the controller only after normal fencing of its predecessor.
    //= spec/impl/v2/implementation.md#shared-memory-changes-reach-sessions
    //# Pending memory-change notices MUST remain recoverable after a process restart.
    // The controller retains the batch; this mirror records delivery ambiguity
    // before context append. An ACK means appended, never model consumption.
    pub(super) fn begin_append(
        &mut self,
        batch_id: &str,
        context: &str,
    ) -> Result<AppendDisposition, String> {
        if !self.current_confirmed
            || !crate::mcp::valid_identity(context, self.client.limits().transport.identity_bytes)
        {
            return Err("memory delivery lacks a current bounded context".into());
        }
        self.safe(&context)?;
        let mut mirror = self.mirror.clone().ok_or("memory delivery batch absent")?;
        let pending = mirror
            .pending
            .as_mut()
            .ok_or("memory delivery batch absent")?;
        if pending.batch.batch_id != batch_id || pending.batch.page.notices.is_empty() {
            return Err("memory delivery batch differs".into());
        }
        if pending
            .delivery
            .as_ref()
            .is_some_and(|d| d.context == context || d.caller == self.caller)
        {
            return Ok(AppendDisposition::Deferred);
        }
        pending.delivery = Some(Delivery {
            caller: self.caller.clone(),
            context: context.into(),
            appended: false,
        });
        self.persist(mirror)?;
        Ok(AppendDisposition::Ready)
    }

    /// Call only after the surrounding required input is known appended/submitted.
    /// This acknowledges delivery, never consumption, and never repeats input.
    pub(super) async fn appended(
        &mut self,
        batch_id: &str,
        context: &str,
        control: &Control,
    ) -> Result<(), String> {
        let mut mirror = self.mirror.clone().ok_or("memory delivery batch absent")?;
        let pending = mirror
            .pending
            .as_mut()
            .ok_or("memory delivery batch absent")?;
        let delivery = pending
            .delivery
            .as_mut()
            .ok_or("memory delivery was not begun")?;
        if pending.batch.batch_id != batch_id
            || delivery.context != context
            || delivery.caller != self.caller
        {
            return Err("memory append acknowledgment context differs".into());
        }
        delivery.appended = true;
        self.persist(mirror)?;
        self.acknowledge(control).await
    }

    async fn acknowledge(&mut self, control: &Control) -> Result<(), String> {
        let pending = self
            .mirror
            .as_ref()
            .and_then(|m| m.pending.as_ref())
            .ok_or("memory acknowledgment batch absent")?
            .clone();
        if !pending.batch.page.notices.is_empty()
            && !pending.delivery.as_ref().is_some_and(|d| d.appended)
        {
            return Err("memory delivery is ambiguous; acknowledgment withheld".into());
        }
        // At most one predecessor (from a stopped caller) and one fresh delivery
        // acknowledgment. Unknown predecessors are never replaced.
        for _ in 0..2 {
            let request = Operation::AcknowledgeAgentMemory {
                operation_id: mutations::fresh_id("memory-ack")?,
                caller: self.caller.clone(),
                batch_id: pending.batch.batch_id.clone(),
            };
            let (path, mut journal) = self.mutation("acknowledge.json", request)?;
            let Operation::AcknowledgeAgentMemory {
                caller, batch_id, ..
            } = &journal.request
            else {
                return Err("unexpected retained memory acknowledgment".into());
            };
            if batch_id != &pending.batch.batch_id || caller.task_id != self.caller.task_id {
                return Err("retained memory acknowledgment binding differs".into());
            }
            let previous_caller = caller != &self.caller;
            match self.resolve(&path, &mut journal, control).await? {
                Outcome::Ok {
                    data: Data::AgentMemory(observation),
                } => {
                    if observation.acknowledged_cursor != pending.batch.page.to_cursor
                        || observation.pending.is_some()
                    {
                        return Err("memory acknowledgment did not commit the exact batch".into());
                    }
                    self.accept(&observation)?;
                    mutations::consume(&path, &mut journal)?;
                    return Ok(());
                }
                Outcome::Refused { .. }
                    if previous_caller
                        && (pending.batch.page.notices.is_empty()
                            || pending
                                .delivery
                                .as_ref()
                                .is_some_and(|d| d.appended && d.caller == self.caller)) =>
                {
                    // A different, successfully delivered occurrence can issue
                    // its own ACK only after the old exact refusal is known.
                    mutations::consume(&path, &mut journal)?;
                }
                Outcome::Refused { .. } => {
                    return Err("memory acknowledgment refused; pending delivery retained".into());
                }
                _ => return Err("unexpected committed memory acknowledgment response".into()),
            }
        }
        Err("memory acknowledgment predecessor unresolved".into())
    }
}

fn scope_directory(state: &Path, session_id: &str) -> PathBuf {
    let encoded = component(session_id);
    let mut path = state.join("memory-notices");
    // Keep every filename below NAME_MAX without truncating identity or hashing.
    for chunk in encoded.as_bytes().chunks(96) {
        path.push(std::str::from_utf8(chunk).expect("hexadecimal component"));
    }
    path
}

fn read_private(path: &Path, limit: usize, field: &str) -> Result<Option<Vec<u8>>, String> {
    // Linux O_NONBLOCK | O_NOFOLLOW: a FIFO cannot stall the worker, and a final
    // symlink cannot redirect validation to a different file than the read.
    let file = match std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(0x800 | 0x20000)
        .open(path)
    {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err("memory notification record unavailable".into()),
    };
    let metadata = file
        .metadata()
        .map_err(|_| "memory notification record metadata unavailable")?;
    let owner = std::fs::metadata("/proc/self")
        .map_err(|_| "memory notification owner unavailable")?
        .uid();
    if !metadata.is_file()
        || metadata.uid() != owner
        || metadata.nlink() != 1
        || metadata.permissions().mode() & 0o777 != 0o600
    {
        return Err("memory notification record is not a bounded private regular file".into());
    }
    if metadata.len() > limit as u64 {
        return Err(format!(
            "memory notification record exceeds bound ({field})"
        ));
    }
    let mut bytes = Vec::new();
    file.take((limit + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| "memory notification record unreadable")?;
    if bytes.len() > limit {
        return Err(format!(
            "memory notification record exceeds bound ({field})"
        ));
    }
    Ok(Some(bytes))
}

#[cfg(test)]
#[path = "memory_notice_tests.rs"]
mod tests;
