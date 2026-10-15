use super::*;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
#[derive(Clone)]
pub struct Client {
    base: String,
    caller: Caller,
    http: reqwest::Client,
    limits: RuntimeLimits,
    sequence: Arc<AtomicU64>,
}
impl Client {
    pub fn new(base: &str, caller: Caller, limits: RuntimeLimits) -> Result<Self, String> {
        limits.validate()?;
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
            sequence: Arc::new(AtomicU64::new(1)),
        })
    }
    async fn exchange(&self, method: &str, params: Value) -> Result<Value, String> {
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
            .map_err(|_| "MCP transport outcome unknown; reconcile original occurrence")?;
        if response.status() != reqwest::StatusCode::OK {
            return Err("MCP HTTP outcome unavailable; reconcile original occurrence".into());
        }
        let mut bytes = vec![];
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| "MCP response incomplete; reconcile original occurrence")?
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
        if result.tools != catalogue(&self.limits) {
            return Err("MCP catalogue differs from supported collaboration tools".into());
        }
        Ok(result.tools)
    }
    async fn tool(&self, name: &str, args: Value, meta: Value) -> Result<CallResult, String> {
        let v = if name == "__host_lookup" {
            self.exchange("borg/calls/get", args).await?
        } else {
            self.exchange(
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
        call.validate(&self.limits)?;
        if call.caller != self.caller {
            return Err("host caller differs from invocation".into());
        }
        let received=self.tool(&call.tool,call.arguments.clone(),json!({"borg":{"operation_id":call.operation_id,"logical_call_id":call.logical_call_id}})).await?;
        let checked = result(call, received.reply)?;
        if received.requires_quiescence != checked.requires_quiescence {
            return Err("MCP quiescence disposition differs".into());
        }
        Ok(checked)
    }
    pub async fn get_call(&self, call: &Invocation) -> Result<Option<CallResult>, String> {
        call.validate(&self.limits)?;
        if call.caller != self.caller {
            return Err("host caller differs from invocation".into());
        }
        let received = self
            .tool(
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
        recovered(call, received.reply).map(Some)
    }
}
