use crate::protocol::{Operation, Reply, Request, WIRE_REVISION};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
#[derive(Clone)]
pub struct ApiClient {
    base: String,
    client: reqwest::Client,
    sequence: Arc<AtomicU64>,
    limits:Arc<crate::limits::RuntimeLimits>,
}
#[derive(Debug)]
pub(crate) enum CallFailure {
    Unavailable(String),
    Contract(String),
}
impl CallFailure {
    pub(crate) fn into_message(self) -> String {
        match self { Self::Unavailable(value) | Self::Contract(value) => value }
    }
}
impl ApiClient {
    //= spec/impl/v2/implementation.md#limits-are-versioned-metadata
    //# A component MUST validate its configured limits before admitting work governed by those limits.
    pub fn new(base: impl Into<String>, limits:Arc<crate::limits::RuntimeLimits>) -> Result<Self, String> {
        limits.validate()?;
        let base = base.into();
        let url = reqwest::Url::parse(&base).map_err(|_| "invalid controller URL")?;
        let local = url.host_str().is_some_and(|host| {
            host == "localhost"
                || host
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback())
        });
        if !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || !(url.scheme() == "https" || (url.scheme() == "http" && local))
        {
            return Err("controller URL must use private loopback forwarding or HTTPS, without embedded credentials".into());
        }
        Ok(Self {
            base: base.trim_end_matches('/').into(),
            client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .retry(reqwest::retry::never())
                .connect_timeout(std::time::Duration::from_millis(limits.transport.client_connect_timeout_ms))
                .timeout(std::time::Duration::from_millis(limits.transport.client_timeout_ms))
                .build()
                .map_err(|e| e.to_string())?,
            sequence: Arc::new(AtomicU64::new(1)),
            limits,
        })
    }
    pub fn limits(&self)->&crate::limits::RuntimeLimits{&self.limits}
    pub async fn call(&self, command: Operation) -> Result<Reply, String> {
        self.call_observed(command).await.map_err(CallFailure::into_message)
    }
    pub(crate) async fn call_observed(&self, command: Operation) -> Result<Reply, CallFailure> {
        let request = Request {
            protocol_revision: WIRE_REVISION,
            request_id: format!("client-{}", self.sequence.fetch_add(1, Ordering::Relaxed)),
            command,
        };
        let bytes = serde_json::to_vec(&request).map_err(|e| CallFailure::Contract(e.to_string()))?;
        if bytes.len() > self.limits.transport.body_bytes {
            return Err(CallFailure::Contract("request exceeds body limit (transport.body_bytes)".into()));
        }
        let mut response = self
            .client
            .post(format!("{}/api/operations", self.base))
            .header("content-type", "application/json")
            .body(bytes)
            .send()
            .await
            .map_err(|e| CallFailure::Unavailable(e.to_string()))?;
        if !response.status().is_success() {
            return Err(CallFailure::Unavailable(format!("controller HTTP {}", response.status())));
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|e| CallFailure::Unavailable(e.to_string()))? {
            if body.len().saturating_add(chunk.len()) > self.limits.transport.body_bytes {
                return Err(CallFailure::Contract("response exceeds body limit (transport.body_bytes)".into()));
            }
            body.extend_from_slice(&chunk);
        }
        let reply: Reply = serde_json::from_slice(&body).map_err(|e| CallFailure::Contract(e.to_string()))?;
        if reply.request_id != request.request_id {
            return Err(CallFailure::Contract("response request identity mismatch".into()));
        }
        reply.validate_for(&request.command).map_err(CallFailure::Contract)?;
        Ok(reply)
    }
}
