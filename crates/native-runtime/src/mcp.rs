//! Streamable HTTP MCP. Session state is scoped to (Native session, server).
//! A tool call is transmitted once; transport loss is returned to the caller,
//! which must reconcile the original operation rather than repeat the mutation.
use crate::model::{McpServerConfig, ToolExecutor};
use reqwest::header::{ACCEPT, CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue};
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

const PROTOCOL: &str = "2025-06-18";
const MAX_REPLY_BYTES: usize = 16 * 1024 * 1024;
const MAX_CATALOG_PAGES: usize = 32;
type SessionKey = (
    String,
    String,
    String,
    Vec<(String, String)>,
    Option<String>,
);
type SharedSession = Arc<Mutex<ServerSession>>;

pub struct McpToolExecutor {
    client: reqwest::Client,
    sessions: Mutex<HashMap<SessionKey, SharedSession>>,
}

impl McpToolExecutor {
    pub fn new() -> anyhow::Result<Self> {
        Self::with_timeout(Duration::from_secs(600))
    }

    pub fn with_timeout(timeout: Duration) -> anyhow::Result<Self> {
        anyhow::ensure!(!timeout.is_zero(), "MCP timeout must be positive");
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(15).min(timeout))
            .timeout(timeout)
            .redirect(reqwest::redirect::Policy::none())
            // reqwest 0.12.15 has implicit HTTP/2 refused-stream retries, but
            // no configurable retry policy. Do not repeat tool transmissions.
            .http1_only()
            .pool_max_idle_per_host(0)
            .build()
            .map_err(|_| anyhow::anyhow!("MCP HTTP client initialization failed"))?;
        Ok(Self {
            client,
            sessions: Mutex::new(HashMap::new()),
        })
    }

    /// Forget local session state after its owner is finished. This sends no
    /// remote request and cannot replay or cancel an outstanding tool operation.
    pub async fn forget_session(&self, session_id: &str) {
        self.sessions
            .lock()
            .await
            .retain(|key, _| key.0 != session_id);
    }

    async fn session(
        &self,
        session_id: &str,
        server: &McpServerConfig,
    ) -> Result<SharedSession, String> {
        if session_id.is_empty() {
            return Err("MCP Native session identity is empty".into());
        }
        let url = reqwest::Url::parse(&server.url).map_err(|_| "MCP server URL is invalid")?;
        if !matches!(url.scheme(), "http" | "https")
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
        {
            return Err("MCP server URL is unsupported".into());
        }
        let key = (
            session_id.to_owned(),
            server.name.clone(),
            server.url.clone(),
            server
                .headers_env
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            server.bound_agent_id.clone(),
        );
        let mut sessions = self.sessions.lock().await;
        Ok(sessions
            .entry(key)
            .or_insert_with(|| {
                Arc::new(Mutex::new(ServerSession {
                    url: server.url.clone(),
                    headers_env: server.headers_env.clone(),
                    bound_agent_id: server.bound_agent_id.clone(),
                    session_header: None,
                    version: None,
                    initialized: false,
                    next_id: 1,
                    tools: BTreeMap::new(),
                }))
            })
            .clone())
    }
}

fn validate_servers(servers: &[McpServerConfig]) -> Result<(), String> {
    let mut names = HashSet::new();
    for server in servers {
        // Keep the namespace delimiter unambiguous. Provider function names are
        // a limited ASCII vocabulary; tool discovery validates the final name.
        if server.name.is_empty()
            || server.name.contains("__")
            || !server.name.bytes().all(name_byte)
            || !names.insert(&server.name)
        {
            return Err("MCP server names must be unique function-safe namespaces".into());
        }
    }
    Ok(())
}

fn name_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-')
}

fn public_name(server: &str, tool: &str) -> Result<String, String> {
    let name = format!("{server}__{tool}");
    if tool.is_empty() || name.len() > 64 || !name.bytes().all(name_byte) {
        return Err("MCP tool name cannot be represented as a provider function".into());
    }
    Ok(name)
}

/// Classify *read-only discovery* errors. Never apply this to tools/call:
/// transport loss after dispatch has an unknown side effect, not a retry grant.
pub(crate) fn discovery_retry_after(error: &str) -> Option<Option<u64>> {
    if matches!(
        error,
        "MCP transport unavailable"
            | "MCP response transport unavailable"
            | "MCP event stream ended before its response"
            | "MCP JSON-RPC error -32603"
    ) {
        return Some(None);
    }
    let tail = error.strip_prefix("MCP HTTP status ")?;
    let (status, hint) = tail
        .split_once("; retry_after_ms=")
        .map_or((tail, None), |(s, h)| (s, h.parse::<u64>().ok()));
    let status: u16 = status.parse().ok()?;
    matches!(status, 404 | 408 | 429 | 500..=599).then_some(hint)
}

fn allowed(server: &McpServerConfig, tool: &str) -> bool {
    server
        .tools_allowlist
        .as_ref()
        .is_none_or(|names| names.iter().any(|n| n == tool))
}

impl ToolExecutor for McpToolExecutor {
    fn list_tools<'a>(
        &'a self,
        session_id: &'a str,
        servers: &'a [McpServerConfig],
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Value>, String>> + Send + 'a>> {
        Box::pin(async move {
            validate_servers(servers)?;
            let mut functions = Vec::new();
            for config in servers {
                let shared = self.session(session_id, config).await?;
                let mut session = shared.lock().await;
                session.initialize(&self.client).await?;
                session.discover(&self.client).await?;
                for (name, tool) in &session.tools {
                    if !allowed(config, name) {
                        continue;
                    }
                    functions.push(json!({
                        "type": "function",
                        "name": public_name(&config.name, name)?,
                        "description": tool.get("description").and_then(Value::as_str).unwrap_or(""),
                        "parameters": tool["inputSchema"],
                        // MCP schemas are not necessarily strict-JSON-schema
                        // subsets. Preserve their actual optional properties.
                        "strict": false
                    }));
                }
            }
            Ok(functions)
        })
    }

    fn call<'a>(
        &'a self,
        session_id: &'a str,
        servers: &'a [McpServerConfig],
        name: &'a str,
        arguments: Value,
        operation_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move {
            validate_servers(servers)?;
            if operation_id.is_empty() {
                return Err("MCP call requires a stable operation identity".into());
            }
            if !arguments.is_object() {
                return known_rejection("tools/call", -32602);
            }
            let Some((server_name, tool_name)) = name.split_once("__") else {
                return known_rejection("tools/call", -32601);
            };
            let Some(config) = servers.iter().find(|s| s.name == server_name) else {
                return known_rejection("tools/call", -32601);
            };
            if !allowed(config, tool_name) {
                return known_rejection("tools/call", -32601);
            }
            let shared = self.session(session_id, config).await?;
            let mut session = shared.lock().await;
            session.initialize(&self.client).await?;
            if !session.tools.contains_key(tool_name) {
                // Tool discovery is safe to perform, but never infer permission
                // from a provider-generated function name alone.
                session.discover(&self.client).await?;
            }
            if !session.tools.contains_key(tool_name) {
                return known_rejection("tools/call", -32601);
            }
            let result = session
                .rpc(
                    &self.client,
                    "tools/call",
                    json!({
                        "name": tool_name,
                        "arguments": arguments,
                        "_meta": {"borg/operation_id": operation_id}
                    }),
                )
                .await?;
            if !result.is_object()
                || !result.get("content").is_some_and(Value::is_array)
                || result.get("isError").is_some_and(|v| !v.is_boolean())
            {
                return Err("MCP tool result has an invalid shape".into());
            }
            // isError is a tool result, not transport uncertainty. Preserve all
            // content/structuredContent/metadata for the model and durable log.
            Ok(result)
        })
    }
}

struct ServerSession {
    url: String,
    headers_env: BTreeMap<String, String>,
    bound_agent_id: Option<String>,
    session_header: Option<HeaderValue>,
    version: Option<String>,
    initialized: bool,
    next_id: u64,
    tools: BTreeMap<String, Value>,
}

impl ServerSession {
    fn headers(&self) -> Result<HeaderMap, String> {
        let mut headers = HeaderMap::new();
        for (name, env) in &self.headers_env {
            let name = HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| "MCP configured header name is invalid")?;
            if name.as_str() == "x-fleet-agent" && self.bound_agent_id.is_some() {
                continue; // Do not even read a stale per-host/per-agent env key.
            }
            if matches!(
                name.as_str(),
                "host"
                    | "content-length"
                    | "transfer-encoding"
                    | "accept"
                    | "content-type"
                    | "mcp-session-id"
                    | "mcp-protocol-version"
            ) {
                return Err("MCP configured header overrides a transport header".into());
            }
            let secret = std::env::var(env).map_err(|_| "MCP header environment is unavailable")?;
            let mut value = HeaderValue::from_str(&secret)
                .map_err(|_| "MCP configured header value is invalid")?;
            value.set_sensitive(true);
            headers.insert(name, value);
        }
        if let Some(agent_id) = &self.bound_agent_id {
            let value = HeaderValue::from_str(agent_id)
                .map_err(|_| "MCP bound agent identity is invalid")?;
            if agent_id.is_empty() || value.to_str().is_err() {
                return Err("MCP bound agent identity is invalid".into());
            }
            headers.insert("x-fleet-agent", value);
        }
        headers.insert(
            ACCEPT,
            HeaderValue::from_static("application/json, text/event-stream"),
        );
        if let Some(session) = &self.session_header {
            headers.insert("mcp-session-id", session.clone());
        }
        if let Some(version) = &self.version {
            headers.insert(
                "mcp-protocol-version",
                HeaderValue::from_str(version).map_err(|_| "MCP negotiated version is invalid")?,
            );
        }
        Ok(headers)
    }

    async fn post(
        &mut self,
        client: &reqwest::Client,
        message: &Value,
    ) -> Result<reqwest::Response, String> {
        let response = client
            .post(&self.url)
            .headers(self.headers()?)
            .json(message)
            .send()
            .await
            .map_err(|_| "MCP transport unavailable")?;
        if !response.status().is_success() {
            let status = response.status().as_u16();
            if status == 404 && self.session_header.is_some() {
                self.initialized = false;
                self.session_header = None;
                self.version = None;
                self.tools.clear();
            }
            // Neither server bodies nor reqwest errors (which can contain URLs
            // or credentials) enter diagnostics.
            let delay = crate::retry::retry_after_ms(response.headers());
            return Err(match delay {
                Some(delay) => format!("MCP HTTP status {status}; retry_after_ms={delay}"),
                None => format!("MCP HTTP status {status}"),
            });
        }
        Ok(response)
    }

    async fn send_oneway(
        &mut self,
        client: &reqwest::Client,
        message: Value,
    ) -> Result<(), String> {
        let response = self.post(client, &message).await?;
        if !matches!(response.status().as_u16(), 202 | 204) {
            return Err("MCP one-way message was not acknowledged".into());
        }
        Ok(())
    }

    async fn initialize(&mut self, client: &reqwest::Client) -> Result<(), String> {
        if self.initialized {
            return Ok(());
        }
        self.session_header = None;
        self.version = None;
        let result = self.rpc(client, "initialize", json!({
            "protocolVersion": PROTOCOL,
            "capabilities": {},
            "clientInfo": {"name": "borg-native-runtime", "version": env!("CARGO_PKG_VERSION")}
        })).await?;
        let version = result
            .get("protocolVersion")
            .and_then(Value::as_str)
            .ok_or("MCP initialization omitted its protocol version")?;
        if !matches!(version, "2025-03-26" | "2025-06-18" | "2025-11-25") {
            return Err("MCP server selected an unsupported protocol version".into());
        }
        if !result.get("capabilities").is_some_and(Value::is_object) {
            return Err("MCP initialization omitted capabilities".into());
        }
        self.version = Some(version.to_owned());
        self.send_oneway(
            client,
            json!({"jsonrpc": "2.0", "method": "notifications/initialized"}),
        )
        .await?;
        self.initialized = true;
        Ok(())
    }

    async fn discover(&mut self, client: &reqwest::Client) -> Result<(), String> {
        let mut tools = BTreeMap::new();
        let mut cursor = None;
        let mut seen_cursors = HashSet::new();
        for _ in 0..MAX_CATALOG_PAGES {
            let params = cursor
                .as_ref()
                .map_or_else(|| json!({}), |c| json!({"cursor": c}));
            let result = self.rpc(client, "tools/list", params).await?;
            let page = result
                .get("tools")
                .and_then(Value::as_array)
                .ok_or("MCP tools/list omitted tools")?;
            for tool in page {
                let name = tool
                    .get("name")
                    .and_then(Value::as_str)
                    .ok_or("MCP tool name is absent")?;
                if name.is_empty()
                    || !tool.get("inputSchema").is_some_and(Value::is_object)
                    || tool.get("description").is_some_and(|v| !v.is_string())
                    || tools.insert(name.to_owned(), tool.clone()).is_some()
                {
                    return Err("MCP catalogue contains an invalid or duplicate tool".into());
                }
            }
            match result.get("nextCursor") {
                None | Some(Value::Null) => {
                    self.tools = tools;
                    return Ok(());
                }
                Some(Value::String(next))
                    if !next.is_empty() && seen_cursors.insert(next.clone()) =>
                {
                    cursor = Some(next.clone());
                }
                _ => return Err("MCP catalogue pagination is invalid".into()),
            }
        }
        Err("MCP catalogue exceeded its bounded page count".into())
    }

    async fn rpc(
        &mut self,
        client: &reqwest::Client,
        method: &str,
        params: Value,
    ) -> Result<Value, String> {
        let id = self.next_id;
        self.next_id = self
            .next_id
            .checked_add(1)
            .ok_or("MCP request counter exhausted")?;
        let request = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params});
        let mut response = self.post(client, &request).await?;
        if method == "initialize"
            && let Some(header) = response.headers().get("mcp-session-id")
        {
            if header.as_bytes().is_empty()
                || !header.as_bytes().iter().all(|b| (0x21..=0x7e).contains(b))
            {
                return Err("MCP session header is invalid".into());
            }
            let mut header = header.clone();
            header.set_sensitive(true);
            self.session_header = Some(header);
        }
        let kind = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|h| h.to_str().ok())
            .and_then(|v| v.split(';').next())
            .unwrap_or("")
            .trim()
            .to_owned();
        if kind == "application/json" {
            let mut bytes = Vec::new();
            while let Some(chunk) = response
                .chunk()
                .await
                .map_err(|_| "MCP response transport unavailable")?
            {
                if bytes.len().saturating_add(chunk.len()) > MAX_REPLY_BYTES {
                    return Err("MCP response exceeds its byte limit".into());
                }
                bytes.extend_from_slice(&chunk);
            }
            let value: Value =
                serde_json::from_slice(&bytes).map_err(|_| "MCP response is not valid JSON")?;
            return match classify_rpc(value, id)? {
                RpcMessage::Response(value) => Ok(value),
                RpcMessage::Rejected(code) => known_rejection(method, code),
                _ => Err("MCP JSON response omitted the requested result".into()),
            };
        }
        if kind != "text/event-stream" {
            return Err("MCP response content type is unsupported".into());
        }
        let mut stream = Sse::default();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| "MCP response transport unavailable")?
        {
            for event in stream.push(&chunk)? {
                // rmcp 3.x sends empty-data priming events with id/retry
                // metadata before the response. These are not JSON messages.
                if event.is_empty() {
                    continue;
                }
                let value =
                    serde_json::from_slice(&event).map_err(|_| "MCP event is not valid JSON")?;
                match classify_rpc(value, id)? {
                    RpcMessage::Response(value) => return Ok(value),
                    RpcMessage::Rejected(code) => return known_rejection(method, code),
                    RpcMessage::Notification => {}
                    RpcMessage::Request { id, method } => {
                        // No sampling/roots capabilities are advertised. Ping is
                        // supported; other server requests receive MethodNotFound.
                        let reply = if method == "ping" {
                            json!({"jsonrpc":"2.0","id":id,"result":{}})
                        } else {
                            json!({"jsonrpc":"2.0","id":id,"error":{"code":-32601,"message":"Unsupported method"}})
                        };
                        self.send_oneway(client, reply).await?;
                    }
                }
            }
        }
        Err("MCP event stream ended before its response".into())
    }
}

enum RpcMessage {
    Response(Value),
    Rejected(i64),
    Notification,
    Request { id: Value, method: String },
}

fn classify_rpc(value: Value, expected_id: u64) -> Result<RpcMessage, String> {
    if value.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return Err("MCP response has an invalid JSON-RPC version".into());
    }
    if let Some(method) = value.get("method").and_then(Value::as_str) {
        if value.get("result").is_some() || value.get("error").is_some() {
            return Err("MCP server message mixes request and response fields".into());
        }
        return match value.get("id") {
            None => Ok(RpcMessage::Notification),
            Some(id) if id.is_string() || id.is_i64() || id.is_u64() => Ok(RpcMessage::Request {
                id: id.clone(),
                method: method.to_owned(),
            }),
            _ => Err("MCP server request identity is invalid".into()),
        };
    }
    if value.get("id") != Some(&json!(expected_id))
        || value.get("result").is_some() == value.get("error").is_some()
    {
        return Err("MCP response identity or outcome is invalid".into());
    }
    if let Some(error) = value.get("error") {
        let code = error
            .get("code")
            .and_then(Value::as_i64)
            .ok_or("MCP error code is invalid")?;
        // Standard JSON-RPC parse/request/method/parameter rejections establish
        // that the requested tool was not executed. Arbitrary server errors and
        // internal errors do not establish this and remain unresolved failures.
        if matches!(code, -32700 | -32600 | -32601 | -32602) {
            return Ok(RpcMessage::Rejected(code));
        }
        return Err(format!("MCP JSON-RPC error {code}"));
    }
    Ok(RpcMessage::Response(value["result"].clone()))
}

fn known_rejection(method: &str, code: i64) -> Result<Value, String> {
    if method != "tools/call" {
        return Err(format!("MCP JSON-RPC error {code}"));
    }
    // Do not expose arbitrary error.message/data. A definite pre-execution
    // rejection is ordinary model feedback, not an unknown external mutation.
    Ok(json!({
        "content": [{"type":"text","text":format!(
            "MCP request rejected ({code}); correct the tool name or arguments."
        )}],
        "isError": true,
        "_meta": {"borg/rejection_code": code}
    }))
}

#[derive(Default)]
struct Sse {
    total: usize,
    line: Vec<u8>,
    data: Vec<u8>,
    pending_cr: bool,
}

impl Sse {
    fn push(&mut self, bytes: &[u8]) -> Result<Vec<Vec<u8>>, String> {
        self.total = self.total.saturating_add(bytes.len());
        if self.total > MAX_REPLY_BYTES {
            return Err("MCP event stream exceeds its byte limit".into());
        }
        let mut events = Vec::new();
        // Process CRLF, bare CR, and LF even across network chunk boundaries.
        for &byte in bytes {
            if self.pending_cr {
                self.pending_cr = false;
                if byte == b'\n' {
                    continue;
                }
            }
            match byte {
                b'\r' | b'\n' => {
                    if self.line.is_empty() {
                        if !self.data.is_empty() {
                            self.data.pop(); // final data-line newline
                            events.push(std::mem::take(&mut self.data));
                        }
                    } else if self.line == b"data" || self.line.starts_with(b"data:") {
                        let value = self.line.get(5..).unwrap_or(&[]);
                        let value = value.strip_prefix(b" ").unwrap_or(value);
                        self.data.extend_from_slice(value);
                        self.data.push(b'\n');
                    }
                    self.line.clear();
                    self.pending_cr = byte == b'\r';
                }
                _ => self.line.push(byte),
            }
        }
        Ok(events)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    struct Reply {
        status: u16,
        kind: &'static str,
        extra: String,
        bytes: Vec<u8>,
        sse_open: bool,
        delay: Duration,
    }
    impl Reply {
        fn json(id: u64, result: Value) -> Self {
            Self {
                status: 200,
                kind: "application/json",
                extra: String::new(),
                bytes: serde_json::to_vec(&json!({"jsonrpc":"2.0","id":id,"result":result}))
                    .unwrap(),
                sse_open: false,
                delay: Duration::ZERO,
            }
        }
        fn initialized(session: &str) -> Self {
            let mut reply = Self::json(
                1,
                json!({
                    "protocolVersion": PROTOCOL, "capabilities": {"tools": {}},
                    "serverInfo": {"name":"fixture","version":"1"}
                }),
            );
            reply.extra = format!("Mcp-Session-Id: {session}\r\n");
            reply
        }
        fn ack() -> Self {
            Self {
                status: 202,
                kind: "application/json",
                extra: String::new(),
                bytes: Vec::new(),
                sse_open: false,
                delay: Duration::ZERO,
            }
        }
        fn tools(id: u64) -> Self {
            Self::json(
                id,
                json!({"tools":[
                    {"name":"read","description":"Read","inputSchema":{"type":"object","properties":{"path":{"type":"string"}}}},
                    {"name":"write","inputSchema":{"type":"object","properties":{}}}
                ]}),
            )
        }
    }
    #[derive(Debug)]
    struct Recorded {
        headers: String,
        body: Value,
    }
    async fn read_request(socket: &mut TcpStream) -> Recorded {
        let mut bytes = Vec::new();
        let end = loop {
            let mut block = [0u8; 4096];
            let count = socket.read(&mut block).await.unwrap();
            assert!(count > 0);
            bytes.extend_from_slice(&block[..count]);
            if let Some(at) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                break at + 4;
            }
        };
        let headers = String::from_utf8(bytes[..end].to_vec())
            .unwrap()
            .to_lowercase();
        let length: usize = headers
            .lines()
            .find_map(|l| l.strip_prefix("content-length:"))
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        while bytes.len() < end + length {
            let mut block = [0u8; 4096];
            let n = socket.read(&mut block).await.unwrap();
            assert!(n > 0);
            bytes.extend_from_slice(&block[..n]);
        }
        Recorded {
            headers,
            body: serde_json::from_slice(&bytes[end..end + length]).unwrap(),
        }
    }
    async fn server(replies: Vec<Reply>) -> (String, tokio::task::JoinHandle<Vec<Recorded>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/mcp", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            let mut recorded = Vec::new();
            for reply in replies {
                let (mut socket, _) = listener.accept().await.unwrap();
                recorded.push(read_request(&mut socket).await);
                tokio::time::sleep(reply.delay).await;
                let framing = if reply.sse_open {
                    "Transfer-Encoding: chunked\r\n".to_owned()
                } else {
                    format!("Content-Length: {}\r\n", reply.bytes.len())
                };
                let head = format!(
                    "HTTP/1.1 {} Test\r\nContent-Type: {}\r\n{}{}\r\n",
                    reply.status, reply.kind, reply.extra, framing
                );
                if socket.write_all(head.as_bytes()).await.is_err() {
                    continue;
                }
                if reply.sse_open {
                    // Exercise CRLF and UTF-8 splits across network chunks, and
                    // deliberately leave the SSE response open after the result.
                    for chunk in reply.bytes.chunks(7) {
                        let frame = format!("{:x}\r\n", chunk.len());
                        if socket.write_all(frame.as_bytes()).await.is_err()
                            || socket.write_all(chunk).await.is_err()
                            || socket.write_all(b"\r\n").await.is_err()
                        {
                            break;
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(350)).await;
                } else {
                    let _ = socket.write_all(&reply.bytes).await;
                }
            }
            assert!(
                tokio::time::timeout(Duration::from_millis(30), listener.accept())
                    .await
                    .is_err(),
                "adapter unexpectedly transmitted an extra HTTP request"
            );
            recorded
        });
        (url, task)
    }
    fn config(url: String) -> McpServerConfig {
        McpServerConfig {
            name: "files".into(),
            url,
            headers_env: BTreeMap::new(),
            bound_agent_id: None,
            tools_allowlist: None,
            allowed_agent_ids: Vec::new(),
        }
    }

    #[tokio::test]
    async fn discovers_pages_preserves_result_and_sends_exact_operation_metadata() {
        let tool_result = json!({"content":[{"type":"text","text":"done"}],
            "structuredContent":{"revision":7},"isError":false,"_meta":{"retained":"original"}});
        let (url, task) = server(vec![
            Reply::initialized("opaque-a"), Reply::ack(),
            Reply::json(2, json!({"tools":[
                {"name":"read","inputSchema":{"type":"object","properties":{}}}
            ],"nextCursor":"page2"})),
            Reply::json(3, json!({"tools":[
                {"name":"write","inputSchema":{"type":"object","properties":{"path":{"type":"string"}}}}
            ]})),
            Reply::json(4, tool_result.clone()),
        ]).await;
        let mut c = config(url);
        c.tools_allowlist = Some(vec!["write".into()]);
        let client = McpToolExecutor::new().unwrap();
        let servers = [c];
        let tools = client.list_tools("native-a", &servers).await.unwrap();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0]["name"], "files__write");
        assert_eq!(
            tools[0]["parameters"]["properties"]["path"]["type"],
            "string"
        );
        assert_eq!(tools[0]["strict"], false);
        let args = json!({"path":"a.txt","contents":"hello"});
        assert_eq!(
            client
                .call(
                    "native-a",
                    &servers,
                    "files__write",
                    args.clone(),
                    "operation-17"
                )
                .await
                .unwrap(),
            tool_result
        );
        assert_eq!(
            client
                .call("native-a", &servers, "files__read", json!({}), "forbidden")
                .await
                .unwrap()["isError"],
            true
        );
        let records = task.await.unwrap();
        assert_eq!(records.len(), 5);
        for record in &records[1..] {
            assert!(record.headers.contains("mcp-session-id: opaque-a\r\n"));
            assert!(
                record
                    .headers
                    .contains("mcp-protocol-version: 2025-06-18\r\n")
            );
        }
        assert_eq!(records[3].body["params"]["cursor"], "page2");
        assert_eq!(records[4].body["params"]["arguments"], args);
        assert_eq!(
            records[4].body["params"]["_meta"]["borg/operation_id"],
            "operation-17"
        );
        assert_eq!(records[4].body["params"]["name"], "write");
    }

    #[tokio::test]
    async fn sse_result_does_not_wait_for_stream_close_and_preserves_tool_error() {
        let result = json!({"content":[{"type":"text","text":"é cannot write"}],"isError":true});
        let mut reply = Reply::json(3, result.clone());
        reply.kind = "text/event-stream; charset=utf-8";
        reply.sse_open = true;
        reply.bytes = format!(
            ": heartbeat\r\n\r\ndata: {{\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\",\"params\":{{}}}}\r\n\r\ndata: {}\r\n\r\n",
            String::from_utf8(reply.bytes).unwrap()
        ).into_bytes();
        let (url, task) = server(vec![
            Reply::initialized("sse-a"),
            Reply::ack(),
            Reply::tools(2),
            reply,
        ])
        .await;
        let client = McpToolExecutor::with_timeout(Duration::from_millis(250)).unwrap();
        let servers = [config(url)];
        client.list_tools("s", &servers).await.unwrap();
        let actual = client
            .call("s", &servers, "files__write", json!({}), "op")
            .await
            .unwrap();
        assert_eq!(actual, result);
        assert_eq!(task.await.unwrap().len(), 4);
    }

    #[tokio::test]
    async fn rmcp_empty_priming_events_before_initialize_and_tools_are_not_json_messages() {
        fn primed(mut reply: Reply, newline: &str) -> Reply {
            let json = String::from_utf8(reply.bytes).unwrap();
            // Captured from imported Task Board's rmcp 3.5.0:
            // data: \nid: 0\nretry: 3000\n\n, then the JSON-RPC event.
            reply.bytes = format!(
                "data: {newline}id: 0{newline}retry: 3000{newline}{newline}data: {json}{newline}{newline}"
            ).into_bytes();
            reply.kind = "text/event-stream";
            reply.sse_open = true;
            reply
        }
        let (url, task) = server(vec![
            primed(Reply::initialized("real-rmcp-shape"), "\n"),
            Reply::ack(),
            primed(Reply::tools(2), "\r\n"),
        ])
        .await;
        let client = McpToolExecutor::with_timeout(Duration::from_secs(2)).unwrap();
        let tools = client.list_tools("n", &[config(url)]).await.unwrap();
        assert_eq!(tools.len(), 2);
        assert_eq!(tools[0]["name"], "files__read");
        let records = task.await.unwrap();
        assert!(
            records[2]
                .headers
                .contains("mcp-session-id: real-rmcp-shape\r\n")
        );
        assert_eq!(records.len(), 3);
    }

    #[tokio::test]
    async fn priming_support_does_not_ignore_malformed_nonempty_events() {
        let mut malformed = Reply::tools(2);
        malformed.kind = "text/event-stream";
        malformed.bytes = b"data: \nid: 0\nretry: 3000\n\ndata: {broken\n\n".to_vec();
        let (url, task) = server(vec![Reply::initialized("s"), Reply::ack(), malformed]).await;
        let client = McpToolExecutor::new().unwrap();
        assert_eq!(
            client.list_tools("n", &[config(url)]).await.unwrap_err(),
            "MCP event is not valid JSON"
        );
        assert_eq!(task.await.unwrap().len(), 3);
    }

    /// Optional real integration check; ordinary tests use the captured wire shape.
    #[tokio::test]
    #[ignore = "requires BORG_TEST_TASK_BOARD_URL for an isolated real Task Board"]
    async fn real_task_board_discovers_tools_in_two_independent_sessions() {
        let url = std::env::var("BORG_TEST_TASK_BOARD_URL").expect("isolated Board URL");
        let mut board = config(url);
        board.name = "board".into();
        let client = McpToolExecutor::with_timeout(Duration::from_secs(10)).unwrap();
        let servers = [board];
        let first = client.list_tools("real-board-a", &servers).await.unwrap();
        let second = client.list_tools("real-board-b", &servers).await.unwrap();
        assert!(first.len() > 100, "expected the real Task Board catalogue");
        assert_eq!(first, second);
        assert!(first.iter().any(|tool| tool["name"] == "board__list_tasks"));
        let sessions = client.sessions.lock().await;
        assert_eq!(sessions.len(), 2);
        let mut ids = HashSet::new();
        for session in sessions.values() {
            let session = session.lock().await;
            ids.insert(session.session_header.as_ref().unwrap().as_bytes().to_vec());
        }
        assert_eq!(ids.len(), 2, "real Board sessions must be isolated");
        eprintln!(
            "real Task Board: {} tools, two distinct MCP sessions",
            first.len()
        );
    }

    #[tokio::test]
    async fn bound_identities_override_host_environment_and_keep_separate_catalogs() {
        let (url, task) = server(vec![
            Reply::initialized("identity-a"),
            Reply::ack(),
            Reply::tools(2),
            Reply::initialized("identity-b"),
            Reply::ack(),
            Reply::tools(2),
        ])
        .await;
        let client = McpToolExecutor::new().unwrap();
        let mut first = config(url.clone());
        first.name = "board".into();
        first.headers_env.insert(
            "X-Fleet-Agent".into(),
            format!("ABSENT_AGENT_ENV_{}", uuid::Uuid::new_v4()),
        );
        first.bound_agent_id = Some("metadata-created-a".into());
        let mut second = first.clone();
        second.bound_agent_id = Some("metadata-created-b".into());
        client.list_tools("native-a", &[first]).await.unwrap();
        client.list_tools("native-b", &[second]).await.unwrap();
        let requests = task.await.unwrap();
        for request in &requests[..3] {
            assert!(
                request
                    .headers
                    .contains("x-fleet-agent: metadata-created-a\r\n")
            );
        }
        for request in &requests[3..] {
            assert!(
                request
                    .headers
                    .contains("x-fleet-agent: metadata-created-b\r\n")
            );
        }
        assert!(
            requests[2]
                .headers
                .contains("mcp-session-id: identity-a\r\n")
        );
        assert!(
            requests[5]
                .headers
                .contains("mcp-session-id: identity-b\r\n")
        );
        assert_eq!(client.sessions.lock().await.len(), 2);
    }

    #[tokio::test]
    async fn native_sessions_never_share_negotiated_mcp_session_headers() {
        let (url, task) = server(vec![
            Reply::initialized("one"),
            Reply::ack(),
            Reply::tools(2),
            Reply::initialized("two"),
            Reply::ack(),
            Reply::tools(2),
        ])
        .await;
        let client = McpToolExecutor::new().unwrap();
        let servers = [config(url)];
        client.list_tools("native-1", &servers).await.unwrap();
        client.list_tools("native-2", &servers).await.unwrap();
        let records = task.await.unwrap();
        assert!(!records[0].headers.contains("mcp-session-id:"));
        assert!(!records[3].headers.contains("mcp-session-id:"));
        assert!(records[2].headers.contains("mcp-session-id: one\r\n"));
        assert!(records[5].headers.contains("mcp-session-id: two\r\n"));
        client.forget_session("native-1").await;
        assert_eq!(client.sessions.lock().await.len(), 1);
    }

    #[tokio::test]
    async fn failed_mutation_is_sent_once_and_diagnostics_never_include_response_body() {
        let mut failure = Reply::json(3, json!({}));
        failure.status = 502;
        failure.bytes = b"SECRET-CREDENTIAL upstream diagnostic body".to_vec();
        let (url, task) = server(vec![
            Reply::initialized("s"),
            Reply::ack(),
            Reply::tools(2),
            failure,
        ])
        .await;
        let client = McpToolExecutor::new().unwrap();
        let servers = [config(url)];
        client.list_tools("n", &servers).await.unwrap();
        let error = client
            .call("n", &servers, "files__write", json!({}), "retained-id")
            .await
            .unwrap_err();
        assert_eq!(error, "MCP HTTP status 502");
        let records = task.await.unwrap();
        assert_eq!(
            records
                .iter()
                .filter(|r| r.body["method"] == "tools/call")
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn definite_rpc_rejections_become_model_visible_tool_results_without_retry() {
        for code in [-32600, -32601, -32602] {
            let mut rejection = Reply::json(3, json!({}));
            rejection.bytes = serde_json::to_vec(&json!({
                "jsonrpc":"2.0", "id":3,
                "error":{"code":code,"message":"SECRET untrusted diagnostic","data":"SECRET"}
            }))
            .unwrap();
            let (url, task) = server(vec![
                Reply::initialized("s"),
                Reply::ack(),
                Reply::tools(2),
                rejection,
            ])
            .await;
            let client = McpToolExecutor::new().unwrap();
            let servers = [config(url)];
            client.list_tools("n", &servers).await.unwrap();
            let result = client
                .call(
                    "n",
                    &servers,
                    "files__write",
                    json!({}),
                    "rejected-original",
                )
                .await
                .unwrap();
            assert_eq!(result["isError"], true);
            assert_eq!(result["_meta"]["borg/rejection_code"], code);
            assert!(!result.to_string().contains("SECRET"));
            let records = task.await.unwrap();
            assert_eq!(
                records
                    .iter()
                    .filter(|r| r.body["method"] == "tools/call")
                    .count(),
                1
            );
        }
    }

    #[tokio::test]
    async fn local_preexecution_rejections_never_send_a_tool_call() {
        let client = McpToolExecutor::new().unwrap();
        let unavailable = [config("http://127.0.0.1:0/mcp".into())];
        for (name, arguments, code) in [
            ("files__write", json!([]), -32602),
            ("missing", json!({}), -32601),
            ("unknown__write", json!({}), -32601),
        ] {
            let result = client
                .call("n", &unavailable, name, arguments, "original")
                .await
                .unwrap();
            assert_eq!(result["_meta"]["borg/rejection_code"], code);
        }
        let (url, task) = server(vec![
            Reply::initialized("s"),
            Reply::ack(),
            Reply::tools(2),
            Reply::tools(3),
        ])
        .await;
        let servers = [config(url)];
        client.list_tools("n", &servers).await.unwrap();
        let result = client
            .call("n", &servers, "files__unknown", json!({}), "original")
            .await
            .unwrap();
        assert_eq!(result["_meta"]["borg/rejection_code"], -32601);
        let records = task.await.unwrap();
        assert!(records.iter().all(|r| r.body["method"] != "tools/call"));
    }

    #[tokio::test]
    async fn timeout_does_not_retransmit_the_mutating_call() {
        let mut delayed = Reply::json(3, json!({"content":[]}));
        delayed.delay = Duration::from_millis(180);
        let (url, task) = server(vec![
            Reply::initialized("s"),
            Reply::ack(),
            Reply::tools(2),
            delayed,
        ])
        .await;
        let client = McpToolExecutor::with_timeout(Duration::from_millis(100)).unwrap();
        let servers = [config(url)];
        client.list_tools("n", &servers).await.unwrap();
        let error = client
            .call("n", &servers, "files__write", json!({}), "original")
            .await
            .unwrap_err();
        assert_eq!(error, "MCP transport unavailable");
        assert_eq!(
            task.await
                .unwrap()
                .iter()
                .filter(|r| r.body["method"] == "tools/call")
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn mismatched_response_and_expired_session_do_not_dispatch_a_second_call() {
        for status in [200, 404] {
            let mut reply = Reply::json(999, json!({"content":[]}));
            reply.status = status;
            let (url, task) = server(vec![
                Reply::initialized("s"),
                Reply::ack(),
                Reply::tools(2),
                reply,
            ])
            .await;
            let client = McpToolExecutor::new().unwrap();
            let servers = [config(url)];
            client.list_tools("n", &servers).await.unwrap();
            let error = client
                .call("n", &servers, "files__write", json!({}), "original")
                .await
                .unwrap_err();
            assert_eq!(
                error,
                if status == 404 {
                    "MCP HTTP status 404"
                } else {
                    "MCP response identity or outcome is invalid"
                }
            );
            assert_eq!(task.await.unwrap().len(), 4);
        }
    }

    #[test]
    fn event_framing_handles_all_newlines_and_rpc_errors_are_sanitized() {
        let mut sse = Sse::default();
        let mut got = Vec::new();
        for byte in b":keepalive\r\ndata: first\rdata: second\r\rdata: third\n\n" {
            got.extend(sse.push(&[*byte]).unwrap());
        }
        assert_eq!(got, vec![b"first\nsecond".to_vec(), b"third".to_vec()]);
        match classify_rpc(
            json!({"jsonrpc":"2.0","id":7,"error":{"code":-32000,"message":"SECRET"}}),
            7,
        ) {
            Err(error) => assert_eq!(error, "MCP JSON-RPC error -32000"),
            _ => panic!("error response was accepted"),
        }
        assert!(classify_rpc(json!({"jsonrpc":"2.0","id":7,"result":{},"error":{}}), 7).is_err());
        assert!(classify_rpc(json!({"jsonrpc":"2.0","id":8,"result":{}}), 7).is_err());
    }
}
