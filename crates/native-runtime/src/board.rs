//! Board is the identity/profile authority; host JSON only supplies connectivity.
//! Reads fail closed (there is no fallback to a stale, host-local agent definition).
use crate::{AgentConfig, BoardConfig, Session, validate_agent};
use anyhow::{Context, Result, bail, ensure};
use reqwest::{Client, Method, Url};
use serde_json::{Map, Value, json};
use std::time::Duration;

#[derive(Debug)]
struct ReadError {
    message: String,
    retryable: bool,
    retry_after_ms: Option<u64>,
}
impl std::fmt::Display for ReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for ReadError {}
pub(crate) fn read_retry_after(error: &anyhow::Error) -> Option<Option<u64>> {
    error
        .downcast_ref::<ReadError>()
        .filter(|e| e.retryable)
        .map(|e| e.retry_after_ms)
}

pub(crate) struct BoardClient {
    client: Client,
    config: BoardConfig,
    base: Url,
}
impl BoardClient {
    pub fn new(config: &BoardConfig) -> Result<Self> {
        let base = Url::parse(&config.url).context("invalid Board URL")?;
        ensure!(
            matches!(base.scheme(), "http" | "https")
                && base.host_str().is_some()
                && base.username().is_empty()
                && base.password().is_none()
                && base.query().is_none()
                && base.fragment().is_none(),
            "unsupported Board URL"
        );
        ensure!(
            config.timeout_ms > 0 && config.report_interval_ms > 0,
            "Board timeouts must be positive"
        );
        let client = Client::builder()
            .timeout(Duration::from_millis(config.timeout_ms))
            .connect_timeout(Duration::from_millis(config.timeout_ms.min(10_000)))
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        Ok(Self {
            client,
            config: config.clone(),
            base,
        })
    }
    fn url(&self, segments: &[&str]) -> Result<Url> {
        let mut url = self.base.clone();
        url.path_segments_mut()
            .map_err(|_| anyhow::anyhow!("invalid Board base URL"))?
            .pop_if_empty()
            .extend(segments);
        Ok(url)
    }
    async fn request(&self, method: Method, url: Url, body: Option<Value>) -> Result<Value> {
        let mut request = self.client.request(method, url);
        if let Some(name) = &self.config.bearer_token_env {
            let token =
                std::env::var(name).context("Board credential environment variable unavailable")?;
            ensure!(!token.is_empty(), "Board credential is empty");
            request = request.bearer_auth(token);
        }
        if let Some(body) = body {
            request = request.json(&body);
        }
        // Do not persist remote error bodies or credentials in local diagnostics.
        let response = request.send().await.map_err(|_| ReadError {
            message: "Board request unavailable".into(),
            retryable: true,
            retry_after_ms: None,
        })?;
        let status = response.status();
        if !status.is_success() {
            return Err(ReadError {
                message: format!("Board request rejected (HTTP {})", status.as_u16()),
                retryable: status.as_u16() == 408
                    || status.as_u16() == 429
                    || status.is_server_error(),
                retry_after_ms: crate::retry::retry_after_ms(response.headers()),
            }
            .into());
        }
        response.json().await.map_err(|error| {
            if error.is_timeout() || error.is_body() || error.is_connect() {
                anyhow::Error::new(ReadError {
                    message: "Board response transport unavailable".into(),
                    retryable: true,
                    retry_after_ms: None,
                })
            } else {
                anyhow::anyhow!("invalid Board response JSON")
            }
        })
    }
    async fn get(&self, segments: &[&str]) -> Result<Value> {
        self.request(Method::GET, self.url(segments)?, None).await
    }
    async fn effective(&self, id: &str, kind: &str) -> Result<Value> {
        let mut url = self.url(&["api", "agents", id, "config", kind])?;
        url.query_pairs_mut().append_pair("effective", "true");
        self.request(Method::GET, url, None).await
    }
    pub async fn load_agent(&self, id: &str) -> Result<AgentConfig> {
        let identity = self.get(&["api", "agents", id]).await?;
        ensure!(
            identity["id"].as_str() == Some(id),
            "Board identity mismatch"
        );
        ensure!(
            identity["retired"] != true && identity["lifecycle_intent"] != "retired",
            "agent is retired: {id}"
        );
        ensure!(
            identity["lifecycle_intent"] != "paused",
            "agent is paused: {id}"
        );
        let (runtime, tools) =
            tokio::try_join!(self.effective(id, "runtime"), self.effective(id, "tool"))?;
        resolve_profile(id, &identity, &runtime, &tools)
    }
    pub async fn check_generation(&self, id: &str, generation: u64) -> Result<()> {
        let state = self.get(&["api", "sessions", id, "state"]).await?;
        ensure!(
            state["generation"]
                .as_u64()
                .is_none_or(|current| generation >= current),
            "session generation superseded on Board; do not replay or dispatch"
        );
        Ok(())
    }
    pub async fn load_agents(&self) -> Result<Vec<AgentConfig>> {
        let mut result = Vec::new();
        let mut offset = 0;
        loop {
            let mut url = self.url(&["api", "agents"])?;
            // Page the actual registry, not a host-maintained allowlist.
            url.query_pairs_mut()
                .append_pair("limit", "200")
                .append_pair("offset", &offset.to_string());
            let page = self.request(Method::GET, url, None).await?;
            let agents = page.as_array().context("invalid Board agent list")?;
            for agent in agents {
                if agent["retired"] == true
                    || agent["lifecycle_intent"] == "retired"
                    || agent["lifecycle_intent"] == "paused"
                {
                    continue;
                }
                // Other harness identities may coexist on the shared board.
                if agent["metadata"]["harness"]
                    .as_str()
                    .is_some_and(|h| h != "native")
                {
                    continue;
                }
                let id = agent["id"].as_str().context("Board agent lacks id")?;
                result.push(self.load_agent(id).await?);
            }
            if agents.len() < 200 {
                break;
            }
            offset += agents.len();
        }
        Ok(result)
    }
    /// Both writes are idempotent. An interrupted delivery can repeat them without
    /// creating another session, overwriting another identity's session or losing a receipt.
    pub async fn report(&self, session: &Session) -> Result<()> {
        self.request(
            Method::POST,
            self.url(&["api", "sessions", &session.id, "state"])?,
            Some(json!({
                "generation": session.generation, "phase": session.phase, "step": session.step,
                "failure_reason": session.error,
            })),
        )
        .await?;
        // A config entry is independently keyed by session id. Never merge a
        // singleton session_id/host onto the reusable agent identity.
        self.request(
            Method::POST,
            self.url(&[
                "api",
                "agents",
                &session.agent_id,
                "config",
                "native_session",
            ])?,
            Some(json!({
                "entry_id": session.id, "enabled": true, "payload": session,
            })),
        )
        .await?;
        Ok(())
    }
}

const PROFILE_KEYS: &[&str] = &[
    "model",
    "instructions",
    "reasoning_effort",
    "max_output_tokens",
    "max_turns",
    "context_window_tokens",
    "max_retries",
    "retry_initial_ms",
    "retry_max_ms",
    "retry_jitter_percent",
    "provider_url",
    "api_key_env",
    "mcp_servers",
    "provider_tools",
];
fn overlay(target: &mut Map<String, Value>, source: &Value) -> Result<()> {
    let source = source
        .as_object()
        .context("native execution profile must be an object")?;
    for key in PROFILE_KEYS {
        if let Some(value) = source.get(*key) {
            target.insert((*key).into(), value.clone());
        }
    }
    if !source.contains_key("reasoning_effort")
        && let Some(effort) = source.get("effort")
    {
        target.insert("reasoning_effort".into(), effort.clone());
    }
    Ok(())
}
fn entries(value: &Value) -> Result<&Vec<Value>> {
    value["entries"]
        .as_array()
        .context("Board effective config lacks entries")
}
fn require_global(entry: &Value) -> Result<()> {
    ensure!(
        entry
            .get("scope")
            .is_none_or(|v| v.is_null() || v.as_array().is_some_and(Vec::is_empty)),
        "scoped native config is unsupported; specify an unscoped grant"
    );
    Ok(())
}
fn resolve_profile(
    id: &str,
    identity: &Value,
    runtime: &Value,
    tools: &Value,
) -> Result<AgentConfig> {
    let mut profile = Map::new();
    for entry in entries(runtime)? {
        if entry["id"] != "profile" {
            continue;
        }
        ensure!(
            entry["enabled"] == true,
            "native runtime profile is disabled"
        );
        require_global(entry)?;
        overlay(&mut profile, &entry["payload"])?;
    }
    if let Some(metadata) = identity.get("metadata").filter(|v| !v.is_null()) {
        overlay(&mut profile, metadata)?;
        if let Some(native) = metadata.get("native").filter(|v| v.is_object()) {
            overlay(&mut profile, native)?;
        }
    }
    profile.insert("id".into(), json!(id));
    let charter = identity["charter"].as_str().unwrap_or_default();
    let extra = profile
        .get("instructions")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let instructions = [charter, extra]
        .into_iter()
        .filter(|s| !s.trim().is_empty())
        .collect::<Vec<_>>()
        .join("\n\n");
    profile.insert("instructions".into(), json!(instructions));
    let tool_entries = entries(tools)?;
    if !tool_entries.is_empty() {
        let mut servers = Vec::new();
        let mut provider_tools = Vec::new();
        for entry in tool_entries {
            if entry["enabled"] != true {
                continue;
            }
            require_global(entry)?;
            let payload = &entry["payload"];
            match payload["kind"].as_str() {
                Some("mcp") => {
                    let mut server = payload
                        .as_object()
                        .context("MCP grant must be an object")?
                        .clone();
                    server.remove("kind");
                    if !server.contains_key("name") {
                        server.insert("name".into(), entry["id"].clone());
                    }
                    servers.push(Value::Object(server));
                }
                Some("provider") => {
                    ensure!(
                        payload["tool"].is_object() && payload["tool"]["type"].is_string(),
                        "invalid provider tool grant"
                    );
                    provider_tools.push(payload["tool"].clone());
                }
                _ => bail!("unsupported native tool grant; use kind mcp or provider"),
            }
        }
        profile.insert("mcp_servers".into(), json!(servers));
        profile.insert("provider_tools".into(), json!(provider_tools));
    }
    profile.insert("profile_source".into(), json!({"authority":"board", "agent_id":id,
        "identity_updated_at":identity.get("updated_at"), "role":runtime["role"],
        "runtime_role_version":runtime["role_version"], "runtime_agent_version":runtime["agent_version"],
        "tool_role_version":tools["role_version"], "tool_agent_version":tools["agent_version"]}));
    let config: AgentConfig =
        serde_json::from_value(Value::Object(profile)).context("invalid Board native profile")?;
    validate_agent(&config)?;
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn profile_precedence_full_limits_and_disabled_tool_grants() {
        let identity = json!({"charter":"identity charter","metadata":{"model":"flat-model","native":{"model":"nested-model"}}});
        let runtime = json!({"entries":[{"id":"profile","enabled":true,"payload":{"model":"role-model","instructions":"role instructions","provider_tools":[{"type":"web_search"}]}}]});
        let tools = json!({"entries":[{"id":"web","enabled":false,"payload":{"kind":"provider","tool":{"type":"web_search"}}}]});
        let config = resolve_profile("new-id", &identity, &runtime, &tools).unwrap();
        assert_eq!(config.model, "nested-model");
        assert_eq!(config.instructions, "identity charter\n\nrole instructions");
        assert_eq!(config.reasoning_effort.as_deref(), Some("max"));
        assert_eq!(config.max_output_tokens, Some(128000));
        assert_eq!(config.context_window_tokens, 1050000);
        assert_eq!(config.max_turns, None);
        assert!(config.provider_tools.is_empty());
    }
    #[test]
    fn malformed_or_scoped_grants_never_fall_back_to_broader_tools() {
        let empty = json!({"entries":[]});
        for tools in [
            json!({"entries":[{"enabled":true,"payload":{}}]}),
            json!({"entries":[{"enabled":true,"scope":["write"],"payload":{"kind":"mcp","name":"board","url":"http://localhost/mcp"}}]}),
        ] {
            assert!(resolve_profile("id", &json!({}), &empty, &tools).is_err());
        }
    }
}
