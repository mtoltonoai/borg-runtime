use crate::model::{BoxFuture, Provider, ProviderError, ProviderRequest, ProviderResponse};
use reqwest::{Client, ClientBuilder, redirect::Policy};
use serde_json::{Value, json};
use std::time::Duration;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);

/// A single-attempt Responses API transport. Retry decisions belong to the runtime.
#[derive(Clone)]
pub struct OpenAiProvider {
    client: Client,
}

impl OpenAiProvider {
    /// Large reasoning responses have no default overall deadline.
    pub fn new() -> Result<Self, anyhow::Error> {
        Self::build(None)
    }

    /// Apply an explicit deadline to the whole request, including its response body.
    pub fn with_timeout(timeout: Duration) -> Result<Self, anyhow::Error> {
        if timeout.is_zero() {
            anyhow::bail!("provider timeout must be positive");
        }
        Self::build(Some(timeout))
    }

    fn builder(timeout: Option<Duration>) -> ClientBuilder {
        let builder = Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .redirect(Policy::none())
            // reqwest 0.12 can transparently retry refused HTTP/2 streams.
            // Avoid those retries and stale pooled connections: one runtime
            // attempt owns one fresh HTTP/1 request.
            .http1_only()
            .pool_max_idle_per_host(0);
        match timeout {
            Some(timeout) => builder.timeout(timeout),
            None => builder,
        }
    }

    fn build(timeout: Option<Duration>) -> Result<Self, anyhow::Error> {
        let client = Self::builder(timeout)
            .build()
            .map_err(|_| anyhow::anyhow!("provider HTTP client initialization failed"))?;
        Ok(Self { client })
    }

    async fn send(
        &self,
        request: ProviderRequest,
        api_key: &str,
    ) -> Result<ProviderResponse, ProviderError> {
        let mut body = json!({
            "model": request.model,
            "instructions": request.instructions,
            "input": request.input,
            "tools": request.tools,
            "store": false,
            "include": ["reasoning.encrypted_content"],
            "truncation": "disabled",
        });
        if let Some(effort) = request.reasoning_effort {
            body["reasoning"] = json!({ "effort": effort });
        }
        if let Some(max_output_tokens) = request.max_output_tokens {
            body["max_output_tokens"] = json!(max_output_tokens);
        }

        // Build separately so malformed URLs/credentials are permanent local
        // configuration failures. Never format reqwest errors: they contain URLs.
        let request = self
            .client
            .post(&request.endpoint)
            .bearer_auth(api_key)
            .json(&body)
            .build()
            .map_err(|_| failure("provider request configuration is invalid", false))?;
        let response = self
            .client
            .execute(request)
            .await
            .map_err(transport_error)?;
        let status = response.status();
        if !status.is_success() {
            // Do not read or expose an error body, even for malformed JSON/HTML.
            return Err(failure(
                format!("provider returned HTTP {}", status.as_u16()),
                status.as_u16() == 429 || status.as_u16() == 408 || status.is_server_error(),
            ));
        }
        let bytes = response.bytes().await.map_err(transport_error)?;
        let value: Value = serde_json::from_slice(&bytes)
            .map_err(|_| failure("provider returned invalid JSON", false))?;
        decode_response(value)
    }
}

impl Provider for OpenAiProvider {
    fn respond(
        &self,
        request: ProviderRequest,
    ) -> BoxFuture<'_, Result<ProviderResponse, ProviderError>> {
        Box::pin(async move {
            let key = std::env::var(&request.api_key_env)
                .ok()
                .filter(|key| !key.trim().is_empty())
                .ok_or_else(|| failure("provider authentication is not configured", false))?;
            self.send(request, &key).await
        })
    }
}

fn failure(message: impl Into<String>, retryable: bool) -> ProviderError {
    ProviderError {
        message: message.into(),
        retryable,
    }
}

fn transport_error(error: reqwest::Error) -> ProviderError {
    failure(
        if error.is_timeout() {
            "provider request timed out"
        } else {
            "provider request transport failed"
        },
        true,
    )
}

fn decode_response(mut value: Value) -> Result<ProviderResponse, ProviderError> {
    let malformed = || failure("provider response has an invalid envelope", false);
    let object = value.as_object_mut().ok_or_else(malformed)?;
    let id = match object.remove("id") {
        None | Some(Value::Null) => None,
        Some(Value::String(id)) => Some(id),
        _ => return Err(malformed()),
    };
    let status = match object.remove("status") {
        Some(Value::String(status)) => status,
        _ => return Err(malformed()),
    };
    let output = match object.remove("output") {
        Some(Value::Array(output)) => output,
        _ => return Err(malformed()),
    };
    // Preserve every output item and all nested fields. In particular, do not
    // parse/re-serialize function arguments or project away encrypted reasoning.
    // Completion/incompletion decisions belong to the runtime.
    Ok(ProviderResponse {
        id,
        status,
        output,
        usage: object.remove("usage"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Router,
        body::Body,
        extract::{DefaultBodyLimit, State},
        http::{HeaderMap, Response},
        routing::post,
    };
    use std::sync::{Arc, Mutex};
    use tokio::{net::TcpListener, task::JoinHandle};

    const KEY: &str = "local-test-auth-secret";

    #[derive(Clone)]
    struct Reply {
        status: u16,
        body: String,
        location: Option<String>,
        delay: Duration,
    }

    #[derive(Clone)]
    struct Recorded {
        authorization: Option<String>,
        body: Value,
    }

    struct Mock {
        endpoint: String,
        requests: Arc<Mutex<Vec<Recorded>>>,
        task: JoinHandle<()>,
    }

    impl Drop for Mock {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    impl Mock {
        async fn start(reply: Reply) -> Self {
            async fn handle(
                State((reply, requests)): State<(Reply, Arc<Mutex<Vec<Recorded>>>)>,
                headers: HeaderMap,
                axum::Json(body): axum::Json<Value>,
            ) -> Response<Body> {
                requests.lock().unwrap().push(Recorded {
                    authorization: headers
                        .get("authorization")
                        .and_then(|value| value.to_str().ok())
                        .map(str::to_owned),
                    body,
                });
                tokio::time::sleep(reply.delay).await;
                let mut response = Response::builder()
                    .status(reply.status)
                    .header("content-type", "application/json");
                if let Some(location) = reply.location {
                    response = response.header("location", location);
                }
                response.body(Body::from(reply.body)).unwrap()
            }

            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!(
                "http://{}/v1/responses?secret=local-endpoint-secret",
                listener.local_addr().unwrap()
            );
            let requests = Arc::new(Mutex::new(Vec::new()));
            let app = Router::new()
                .route("/v1/responses", post(handle))
                .layer(DefaultBodyLimit::disable())
                .with_state((reply, requests.clone()));
            let task = tokio::spawn(async move {
                axum::serve(listener, app).await.unwrap();
            });
            Self {
                endpoint,
                requests,
                task,
            }
        }
    }

    fn provider(timeout: Option<Duration>) -> OpenAiProvider {
        // Local mocks must never use an environment-configured external proxy.
        OpenAiProvider {
            client: OpenAiProvider::builder(timeout).no_proxy().build().unwrap(),
        }
    }

    fn request(endpoint: &str) -> ProviderRequest {
        ProviderRequest {
            model: "gpt-6-astra".into(),
            instructions: "Keep the original output.".into(),
            input: vec![json!({"role": "user", "content": "hello"})],
            tools: vec![],
            reasoning_effort: None,
            max_output_tokens: None,
            endpoint: endpoint.into(),
            api_key_env: "UNUSED_BY_LOCAL_TRANSPORT_TEST".into(),
        }
    }

    fn reply(status: u16, body: Value) -> Reply {
        Reply {
            status,
            body: body.to_string(),
            location: None,
            delay: Duration::ZERO,
        }
    }

    #[tokio::test]
    async fn retains_full_output_and_sends_max_reasoning_without_input_truncation() {
        let output = json!([
            {
                "type": "reasoning",
                "id": "reasoning-1",
                "summary": [{"type": "summary_text", "text": "retained"}],
                "encrypted_content": "opaque+encrypted/bytes==\n\u{2603}",
                "future_reasoning_field": {"keep": [1, true, null]}
            },
            {
                "type": "function_call",
                "id": "function-1",
                "call_id": "original-call",
                "name": "document_update",
                "arguments": "{\n  \"text\": \"hello\\\\world\", \"number\": 1e0\n}",
                "status": "completed"
            },
            {
                "type": "message",
                "role": "assistant",
                "content": [{"type": "output_text", "text": "done", "annotations": []}]
            },
            {"type": "future_output_type", "unknown": {"nested": ["unchanged"]}}
        ]);
        let usage = json!({
            "input_tokens": 900001,
            "output_tokens": 128000,
            "output_tokens_details": {"reasoning_tokens": 127000},
            "future_usage": [1, 2]
        });
        let mock = Mock::start(reply(
            200,
            json!({"id": "response-original", "status": "incomplete", "output": output, "usage": usage}),
        ))
        .await;
        let mut request = request(&mock.endpoint);
        request.reasoning_effort = Some("max".into());
        request.max_output_tokens = Some(128000);
        request.input = vec![json!({
            "role": "user",
            "content": "context ".repeat(150000)
        })];
        request.tools = vec![json!({
            "type": "function", "name": "document_update",
            "parameters": {"type": "object", "properties": {}}
        })];
        let expected_input = request.input.clone();
        let expected_tools = request.tools.clone();
        let response = provider(None).send(request, KEY).await.unwrap();
        assert_eq!(response.id.as_deref(), Some("response-original"));
        assert_eq!(response.status, "incomplete");
        assert_eq!(response.output, output.as_array().unwrap().clone());
        assert_eq!(response.usage, Some(usage));
        let requests = mock.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].authorization.as_deref(),
            Some("Bearer local-test-auth-secret")
        );
        let body = &requests[0].body;
        assert_eq!(body["model"], "gpt-6-astra");
        assert_eq!(body["instructions"], "Keep the original output.");
        assert_eq!(body["input"], json!(expected_input));
        assert_eq!(body["tools"], json!(expected_tools));
        assert_eq!(body["reasoning"], json!({"effort": "max"}));
        assert_eq!(body["max_output_tokens"], 128000);
        assert_eq!(body["truncation"], "disabled");
        assert_eq!(body["store"], false);
        assert_eq!(body["include"], json!(["reasoning.encrypted_content"]));
        assert!(body.get("max_input_tokens").is_none());
    }

    #[tokio::test]
    async fn preserves_completed_response_and_omits_unconfigured_limits() {
        let mock = Mock::start(reply(
            200,
            json!({"status": "completed", "output": [], "usage": null}),
        ))
        .await;
        let response = provider(None)
            .send(request(&mock.endpoint), KEY)
            .await
            .unwrap();
        assert_eq!(response.status, "completed");
        assert_eq!(response.id, None);
        assert_eq!(response.usage, Some(Value::Null));
        let requests = mock.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].body.get("reasoning").is_none());
        assert!(requests[0].body.get("max_output_tokens").is_none());
    }

    #[tokio::test]
    async fn classifies_http_status_without_exposing_body_or_retrying() {
        for (status, retryable) in [
            (400, false),
            (401, false),
            (403, false),
            (404, false),
            (408, true),
            (422, false),
            (429, true),
            (500, true),
            (502, true),
            (503, true),
            (504, true),
        ] {
            let mock = Mock::start(Reply {
                body: "invalid JSON body-secret local-test-auth-secret local-endpoint-secret"
                    .into(),
                ..reply(status, Value::Null)
            })
            .await;
            let error = provider(None)
                .send(request(&mock.endpoint), KEY)
                .await
                .unwrap_err();
            assert_eq!(error.retryable, retryable, "HTTP {status}");
            assert_eq!(error.message, format!("provider returned HTTP {status}"));
            assert_eq!(mock.requests.lock().unwrap().len(), 1);
        }
    }

    #[tokio::test]
    async fn redirects_do_not_forward_credentials_or_replay_post() {
        let target = Mock::start(reply(200, json!({"status": "completed", "output": []}))).await;
        for status in [301, 302, 303, 307, 308] {
            let source = Mock::start(Reply {
                location: Some(target.endpoint.clone()),
                ..reply(status, json!({"secret": "body-secret"}))
            })
            .await;
            let error = provider(None)
                .send(request(&source.endpoint), KEY)
                .await
                .unwrap_err();
            assert!(!error.retryable);
            assert_eq!(error.message, format!("provider returned HTTP {status}"));
            assert_eq!(source.requests.lock().unwrap().len(), 1);
            assert!(target.requests.lock().unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn explicit_timeout_returns_one_sanitized_retryable_failure() {
        let mock = Mock::start(Reply {
            delay: Duration::from_secs(2),
            ..reply(200, json!({"status": "completed", "output": []}))
        })
        .await;
        let error = provider(Some(Duration::from_millis(250)))
            .send(request(&mock.endpoint), KEY)
            .await
            .unwrap_err();
        assert!(error.retryable);
        assert_eq!(error.message, "provider request timed out");
        assert_eq!(mock.requests.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn malformed_success_and_local_configuration_errors_are_sanitized() {
        for body in [
            "invalid-json body-secret".to_owned(),
            json!({"status": "completed", "output": {"secret": "body-secret"}}).to_string(),
        ] {
            let mock = Mock::start(Reply {
                body,
                ..reply(200, Value::Null)
            })
            .await;
            let error = provider(None)
                .send(request(&mock.endpoint), KEY)
                .await
                .unwrap_err();
            assert!(!error.retryable);
            assert!(!error.message.contains("secret"));
            assert_eq!(mock.requests.lock().unwrap().len(), 1);
        }
        let error = provider(None)
            .send(request("://local-endpoint-secret"), KEY)
            .await
            .unwrap_err();
        assert!(!error.retryable);
        assert_eq!(error.message, "provider request configuration is invalid");
        let mut absent_key = request("http://127.0.0.1:1");
        absent_key.api_key_env = format!("ABSENT_PROVIDER_KEY_{}", uuid::Uuid::new_v4());
        let error = provider(None).respond(absent_key).await.unwrap_err();
        assert!(!error.retryable);
        assert_eq!(error.message, "provider authentication is not configured");
    }

    #[test]
    fn constructors_accept_optional_deadline_and_reject_zero() {
        assert!(OpenAiProvider::new().is_ok());
        assert!(OpenAiProvider::with_timeout(Duration::from_secs(600)).is_ok());
        assert!(OpenAiProvider::with_timeout(Duration::ZERO).is_err());
    }
}
