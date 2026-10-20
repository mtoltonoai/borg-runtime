use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, future::Future, path::PathBuf, pin::Pin};

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeConfig {
    pub database_path: PathBuf,
    #[serde(default = "default_listen")]
    pub listen: String,
    #[serde(default)]
    pub bearer_token_env: Option<String>,
    pub agents: Vec<AgentConfig>,
}
fn default_listen() -> String {
    "127.0.0.1:8787".into()
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentConfig {
    pub id: String,
    #[serde(default = "default_model")]
    pub model: String,
    pub instructions: String,
    #[serde(default = "default_effort")]
    pub reasoning_effort: Option<String>,
    #[serde(default = "default_output")]
    pub max_output_tokens: Option<u32>,
    #[serde(default)]
    pub max_turns: Option<u64>,
    #[serde(default = "default_context")]
    pub context_window_tokens: u64,
    #[serde(default = "default_retries")]
    pub max_retries: u32,
    #[serde(default = "default_retry_ms")]
    pub retry_initial_ms: u64,
    #[serde(default = "default_provider_url")]
    pub provider_url: String,
    #[serde(default = "default_api_key_env")]
    pub api_key_env: String,
    #[serde(default)]
    pub mcp_servers: Vec<McpServerConfig>,
    #[serde(default)]
    pub provider_tools: Vec<Value>,
}
fn default_model() -> String {
    "gpt-6-astra".into()
}
fn default_effort() -> Option<String> {
    Some("max".into())
}
fn default_output() -> Option<u32> {
    Some(128000)
}
fn default_context() -> u64 {
    1050000
}
fn default_retries() -> u32 {
    3
}
fn default_retry_ms() -> u64 {
    500
}
fn default_provider_url() -> String {
    "https://api.openai.com/v1/responses".into()
}
fn default_api_key_env() -> String {
    "OPENAI_API_KEY".into()
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpServerConfig {
    pub name: String,
    pub url: String,
    #[serde(default)]
    pub headers_env: BTreeMap<String, String>,
    #[serde(default)]
    pub tools_allowlist: Option<Vec<String>>,
}
#[derive(Debug, Clone)]
pub struct ProviderRequest {
    pub model: String,
    pub instructions: String,
    pub input: Vec<Value>,
    pub tools: Vec<Value>,
    pub reasoning_effort: Option<String>,
    pub max_output_tokens: Option<u32>,
    pub endpoint: String,
    pub api_key_env: String,
}
#[derive(Debug, Clone)]
pub struct ProviderResponse {
    pub id: Option<String>,
    pub output: Vec<Value>,
    pub usage: Option<Value>,
    pub status: String,
}
#[derive(Debug, Clone)]
pub struct ProviderError {
    pub message: String,
    pub retryable: bool,
}
impl std::fmt::Display for ProviderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for ProviderError {}
pub trait Provider: Send + Sync {
    fn respond(
        &self,
        request: ProviderRequest,
    ) -> BoxFuture<'_, Result<ProviderResponse, ProviderError>>;
}
pub trait ToolExecutor: Send + Sync {
    fn list_tools<'a>(
        &'a self,
        session_id: &'a str,
        servers: &'a [McpServerConfig],
    ) -> BoxFuture<'a, Result<Vec<Value>, String>>;
    fn call<'a>(
        &'a self,
        session_id: &'a str,
        servers: &'a [McpServerConfig],
        name: &'a str,
        arguments: Value,
        operation_id: &'a str,
    ) -> BoxFuture<'a, Result<Value, String>>;
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SessionStatus {
    Ready,
    Running,
    Retrying,
    Waiting,
    Stopping,
    Stopped,
    Completed,
    Failed,
    NeedsResolution,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Session {
    pub id: String,
    pub agent_id: String,
    pub task: Option<String>,
    pub status: SessionStatus,
    pub progress: String,
    pub error: Option<String>,
    pub turns: u64,
    pub created_at: u64,
    pub updated_at: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TranscriptEntry {
    pub sequence: i64,
    pub kind: String,
    pub item: Value,
    pub created_at: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCall {
    pub call_id: String,
    pub operation_id: String,
    pub name: String,
    pub arguments: Value,
    pub status: String,
    pub result: Option<Value>,
    pub error: Option<String>,
}
