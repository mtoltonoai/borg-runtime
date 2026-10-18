use super::*;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
/// Retry eligibility is established at the actual transport boundary, never by
/// parsing diagnostic text. Complete protocol/receipt errors are held failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CallFailure {
    Transport(String),
    Contract(String),
}
impl CallFailure {
    pub(crate) fn message(&self) -> &str {
        match self { Self::Transport(value) | Self::Contract(value) => value }
    }
    pub(crate) fn into_message(self) -> String {
        match self { Self::Transport(value) | Self::Contract(value) => value }
    }
}
impl From<String> for CallFailure {
    fn from(value: String) -> Self { Self::Contract(value) }
}
impl From<&str> for CallFailure {
    fn from(value: &str) -> Self { Self::Contract(value.into()) }
}

#[derive(Clone)]
pub struct Client {
    base: String,
    caller: Caller,
    http: reqwest::Client,
    limits: RuntimeLimits,
    profile: ExecutionProfile,
    instruction_source: Option<RepositoryReadSource>,
    sequence: Arc<AtomicU64>,
}
impl Client {
    pub fn new(base: &str, caller: Caller, limits: RuntimeLimits) -> Result<Self, String> {
        Self::new_for_profile(base, caller, limits, ExecutionProfile::CollaborationV1 { web_search: None })
    }
    /// The accepted task selects this profile; a server response cannot widen it.
    pub fn new_for_profile(
        base: &str,
        caller: Caller,
        limits: RuntimeLimits,
        profile: ExecutionProfile,
    ) -> Result<Self, String> {
        Self::new_for_task(base, caller, limits, profile, None)
    }
    /// Host-derived accepted scope, never selected by an MCP response.
    pub fn new_for_task(
        base: &str, caller: Caller, limits: RuntimeLimits, profile: ExecutionProfile,
        instruction_source: Option<RepositoryReadSource>,
    ) -> Result<Self, String> {
        limits.validate()?;
        if let Some(source) = &instruction_source { source.validate_instruction_binding(&limits)?; }
        caller.validate(&limits.transport)?;
        if serde_json::to_vec(&caller)
            .map_err(|_| "caller encoding failed")?
            .len()
            > limits.interface.mcp_caller_header_bytes
        {
            return Err("caller exceeds configured MCP header bound".into());
        }
        let url = reqwest::Url::parse(base).map_err(|_| "invalid collaboration URL")?;
        let local = url.host_str().is_some_and(|h| {
            h == "localhost"
                || h.parse::<std::net::IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback())
        });
        if url.scheme() != "http"
            || !local
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || url.path() != "/"
        {
            return Err(
                "collaboration requires the configured private loopback controller origin".into(),
            );
        }
        let http = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            // The stdio bridge pauses its current-thread reactor between frames.
            // Never retain idle sockets across that pause: a controller may close
            // one before the reactor observes it. This is not a resend policy.
            .pool_max_idle_per_host(0)
            .connect_timeout(std::time::Duration::from_millis(
                limits.transport.client_connect_timeout_ms,
            ))
            .timeout(std::time::Duration::from_millis(
                limits.transport.client_timeout_ms,
            ))
            .build()
            .map_err(|_| "collaboration HTTP configuration failed")?;
        Ok(Self {
            base: base.trim_end_matches('/').into(),
            caller,
            http,
            limits,
            profile,
            instruction_source,
            sequence: Arc::new(AtomicU64::new(1)),
        })
    }
    async fn exchange(&self, method: &str, params: Value) -> Result<Value, String> {
        self.exchange_observed(method, params).await.map_err(CallFailure::into_message)
    }
    async fn exchange_observed(&self, method: &str, params: Value) -> Result<Value, CallFailure> {
        let id = format!("borg-mcp-{}", self.sequence.fetch_add(1, Ordering::Relaxed));
        let body =
            serde_json::to_vec(&json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}))
                .map_err(|_| "MCP encoding failed")?;
        if body.len() > self.limits.transport.body_bytes {
            return Err("MCP request bound".into());
        }
        let mut response = self
            .http
            .post(format!("{}/mcp", self.base))
            .header("content-type", "application/json")
            .header("accept", "application/json, text/event-stream")
            .header("mcp-protocol-version", VERSION)
            .header(
                CALLER_HEADER,
                serde_json::to_string(&self.caller).map_err(|_| "caller encoding failed")?,
            )
            .body(body)
            .send()
            .await
            .map_err(|_| CallFailure::Transport("MCP transport outcome unknown; reconcile original occurrence".into()))?;
        if response.status() != reqwest::StatusCode::OK {
            return Err("MCP HTTP outcome unavailable; reconcile original occurrence".into());
        }
        let mut bytes = vec![];
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| CallFailure::Transport("MCP response incomplete; reconcile original occurrence".into()))?
        {
            if bytes.len().saturating_add(chunk.len()) > self.limits.transport.body_bytes {
                return Err("MCP response bound; reconcile original occurrence".into());
            }
            bytes.extend_from_slice(&chunk);
        }
        let value = strict::parse(&bytes, self.limits.transport.body_bytes)?;
        let obj = value.as_object().ok_or("invalid MCP response")?;
        if obj.len() != 3
            || obj.get("jsonrpc") != Some(&json!("2.0"))
            || obj.get("id") != Some(&json!(id))
        {
            return Err("MCP response correlation differs".into());
        }
        obj.get("result")
            .cloned()
            .ok_or_else(|| "MCP method refused; reconcile original occurrence".into())
    }
    pub async fn tools(&self) -> Result<Vec<Tool>, String> {
        let initialized=self.exchange("initialize",json!({"protocolVersion":VERSION,"capabilities":{},"clientInfo":{"name":"borg-native","version":"1"}})).await?;
        if initialized.get("protocolVersion") != Some(&json!(VERSION)) {
            return Err("MCP version differs".into());
        }
        let result = self.exchange("tools/list", json!({})).await?;
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Tools {
            tools: Vec<Tool>,
        }
        let result: Tools =
            serde_json::from_value(result).map_err(|_| "invalid bounded tool catalogue")?;
        // Both clients share the controller-owned closed catalogue, not model
        // or remote-server authority to expand local operations.
        let mut selected = self.limits.clone();
        // Old tasks retain their exact catalogue. For opted-in metadata obtain
        // the canonical accepted definition/override, bound to this attempt.
        if selected.identity_administration.is_some() || selected.task_creation.is_some() {
            let api = crate::client::ApiClient::new(&self.base, Arc::new(self.limits.clone()))?;
            let reply = api.call(Operation::GetTask { task_id:self.caller.task_id.clone() }).await?;
            let Outcome::Ok { data:Data::Task(task) } = reply.outcome else { return Err("accepted MCP task unavailable".into()); };
            let a=task.attempt.as_ref().ok_or("accepted MCP attempt absent")?;
            if a.id!=self.caller.attempt_id || a.generation!=self.caller.generation || a.worker_id!=self.caller.worker_id || a.worker_incarnation!=self.caller.incarnation {
                return Err("accepted MCP caller changed".into());
            }
            let snapshot=task.input.resolved_limits.as_ref().ok_or("accepted MCP metadata absent")?;
            if *snapshot.global_metadata!=self.limits || task.input.definition.execution_profile.as_ref()!=Some(&self.profile) {
                return Err("accepted MCP metadata changed".into());
            }
            selected.execution.collaboration.capabilities=task.input.execution_limits()?.collaboration.capabilities.clone();
        }
        if result.tools != catalogue_for_task(&selected, Some(&self.profile), self.instruction_source.as_ref()) {
            return Err("MCP catalogue differs from supported collaboration tools".into());
        }
        Ok(result.tools)
    }
    async fn tool_observed(&self, name: &str, args: Value, meta: Value) -> Result<CallResult, CallFailure> {
        let v = if name == "__host_lookup" {
            self.exchange_observed("borg/calls/get", args).await?
        } else {
            self.exchange_observed(
                "tools/call",
                json!({"name":name,"arguments":args,"_meta":meta}),
            )
            .await?
        };
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Text {
            #[serde(rename = "type")]
            kind: String,
            text: String,
        }
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Output {
            content: Vec<Text>,
            #[serde(rename = "isError")]
            is_error: bool,
        }
        let value: Output = serde_json::from_value(v).map_err(|_| "invalid MCP tool result")?;
        if value.content.len() != 1 || value.content[0].kind != "text" {
            return Err("invalid MCP content".into());
        }
        let result: CallResult = serde_json::from_value(strict::parse(
            value.content[0].text.as_bytes(),
            self.limits.transport.body_bytes,
        )?)
        .map_err(|_| "invalid controller tool reply")?;
        if value.is_error == matches!(result.reply.outcome, Outcome::Ok { .. }) {
            return Err("MCP outcome discriminator differs".into());
        }
        Ok(result)
    }
    pub async fn call(&self, call: &Invocation) -> Result<CallResult, String> {
        self.call_observed(call).await.map_err(CallFailure::into_message)
    }
    pub(crate) async fn call_observed(&self, call: &Invocation) -> Result<CallResult, CallFailure> {
        call.validate(&self.limits)?;
        if call.caller != self.caller {
            return Err("host caller differs from invocation".into());
        }
        let received=self.tool_observed(&call.tool,call.arguments.clone(),json!({"borg":{"operation_id":call.operation_id,"logical_call_id":call.logical_call_id}})).await?;
        let checked = result(call, received.reply)?;
        if received.requires_quiescence != checked.requires_quiescence {
            return Err("MCP quiescence disposition differs".into());
        }
        Ok(checked)
    }
    pub async fn get_call(&self, call: &Invocation) -> Result<Option<CallResult>, String> {
        self.get_call_observed(call).await.map_err(CallFailure::into_message)
    }
    pub(crate) async fn get_call_observed(&self, call: &Invocation) -> Result<Option<CallResult>, CallFailure> {
        call.validate(&self.limits)?;
        if call.caller != self.caller {
            return Err("host caller differs from invocation".into());
        }
        let received = self
            .tool_observed(
                "__host_lookup",
                json!({"call_id":call.operation_id}),
                json!({}),
            )
            .await?;
        if received.requires_quiescence {
            return Err("lookup is not new delegation authority".into());
        }
        received
            .reply
            .validate_for(&Operation::GetCollaborationCall {
                caller: self.caller.clone(),
                operation_id: call.operation_id.clone(),
            })?;
        if matches!(&received.reply.outcome,Outcome::Refused{code,..} if code=="not_found")
            && received.reply.commit.is_none()
            && received.reply.observed_controller_revision.is_some()
        {
            return Ok(None);
        }
        recovered(call, received.reply).map(Some).map_err(CallFailure::Contract)
    }
}

#[cfg(test)]
#[path = "client_idle_tests.rs"]
mod idle_connection_tests;
