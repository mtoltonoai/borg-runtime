//! Bounded GitHub transport for an already accepted publication target.
//!
//! This module never decides whether a mutation may be sent. The durable worker
//! phase owns that barrier; `reconcile` performs reads only and never falls back
//! to create. Provider response objects are extensible, but retained proofs use
//! the closed, exact publication contract.
use super::*;
use crate::pull_request::{self as pr, Bounds, BranchProof, Intent, Proof, Target};
use reqwest::{Method, StatusCode, Url, header::{HeaderValue, AUTHORIZATION, LINK}};

pub(super) struct Provider {
    target: Target,
    client: reqwest::Client,
    authorization: HeaderValue,
    credentials: Vec<String>,
    limits: Arc<RuntimeLimits>,
    bounds: Bounds,
}

impl Provider {
    pub(super) fn configured(target: &Target, limits: Arc<RuntimeLimits>) -> Result<Self, String> {
        // Configuration names an existing supported input. No ambient token
        // discovery, credential fallback, or repository inference is permitted.
        if !matches!(target.credential_name.as_str(), "GH_TOKEN" | "GITHUB_TOKEN") {
            return Err("pull request credential selector is unsupported".into());
        }
        let token = std::env::var(&target.credential_name)
            .map_err(|_| "pull request configured credential unavailable")?;
        let credentials = configured_credentials(&limits)?;
        Self::with_credential(target, limits, token, credentials)
    }

    fn with_credential(target: &Target, limits: Arc<RuntimeLimits>, token: String,
                       mut credentials: Vec<String>) -> Result<Self, String> {
        let bounds = Bounds::from_limits(&limits);
        pr::validate_target(target, &bounds)?;
        if token.is_empty() || token.len() > limits.transport.body_bytes {
            return Err("pull request configured credential has invalid bound".into());
        }
        let mut authorization = HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(|_| "pull request configured credential has invalid encoding")?;
        authorization.set_sensitive(true);
        if !credentials.iter().any(|value| value == &token) { credentials.push(token); }
        let url = Url::parse(&target.api_base).map_err(|_| "pull request API origin invalid")?;
        let local = url.host_str().is_some_and(|host| host == "localhost"
            || host.trim_start_matches('[').trim_end_matches(']')
                .parse::<std::net::IpAddr>().is_ok_and(|ip| ip.is_loopback()));
        let mut builder = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .connect_timeout(Duration::from_millis(limits.transport.client_connect_timeout_ms))
            .timeout(Duration::from_millis(limits.transport.client_timeout_ms));
        if local { builder = builder.no_proxy(); }
        let client = builder.build().map_err(|_| "pull request HTTP client unavailable")?;
        Ok(Self { target: target.clone(), client, authorization, credentials, limits, bounds })
    }

    fn endpoint(&self, suffix: &[&str]) -> Result<Url, String> {
        let mut url = Url::parse(&self.target.api_base).map_err(|_| "pull request API origin invalid")?;
        {
            let mut path = url.path_segments_mut().map_err(|_| "pull request API path invalid")?;
            path.pop_if_empty();
            path.extend(["repos", self.target.owner.as_str(), self.target.repository.as_str()]);
            path.extend(suffix.iter().copied());
        }
        Ok(url)
    }

    fn scan(&self, value: &serde_json::Value) -> Result<(), String> {
        if value_contains_credential(value, &self.credentials, &self.limits.worker)
            .map_err(|_| "pull request response exceeds credential inspection bounds")? {
            return Err("pull request value contains a configured credential".into());
        }
        Ok(())
    }

    async fn exchange(&self, method: Method, url: Url, body: Option<serde_json::Value>,
                      expected: StatusCode, control: &Control) -> Result<(serde_json::Value, bool), String> {
        control.check()?;
        if control.runtime_limits.as_ref() != self.limits.as_ref() {
            return Err("pull request HTTP policy differs from accepted command".into());
        }
        let bytes = if let Some(body) = body {
            self.scan(&body)?;
            bounded_json_size(&body, self.limits.transport.body_bytes)
                .map_err(|_| "pull request body exceeds transport.body_bytes")?;
            Some(serde_json::to_vec(&body).map_err(|_| "pull request body encoding failed")?)
        } else { None };
        let mut request = self.client.request(method, url)
            .header(AUTHORIZATION, self.authorization.clone())
            .header("accept", "application/vnd.github+json")
            .header("x-github-api-version", "2026-03-10")
            .header("user-agent", "borg-runtime");
        if let Some(bytes) = bytes { request = request.header("content-type", "application/json").body(bytes); }
        let work = async {
            let mut response = request.send().await.map_err(|_| "pull request transport outcome unknown")?;
            if response.status() != expected {
                // A status, including a refusal, is not an absent-effect proof.
                // Do not retain arbitrary provider error bodies or echo URLs.
                return Err(format!("pull request HTTP status {} requires original-effect reconciliation", response.status().as_u16()));
            }
            let headers = response.headers();
            let header_bytes = headers.iter().try_fold(0usize, |total, (key, value)|
                total.checked_add(key.as_str().len()).and_then(|n| n.checked_add(value.as_bytes().len())))
                .ok_or("pull request response headers overflow")?;
            if headers.len() > self.limits.transport.http_header_count
                || header_bytes > self.limits.transport.http_header_bytes {
                return Err("pull request response headers exceed accepted bounds".into());
            }
            // Never follow a provider-supplied URL. A bounded partial page is
            // insufficient even if it happens to contain one matching object.
            let linked = headers.get_all(LINK).iter().next().is_some();
            let mut bytes = Vec::new();
            while let Some(chunk) = response.chunk().await.map_err(|_| "pull request response incomplete")? {
                if bytes.len().saturating_add(chunk.len()) > self.limits.transport.body_bytes {
                    return Err("pull request response exceeds transport.body_bytes".into());
                }
                bytes.extend_from_slice(&chunk);
            }
            let value: serde_json::Value = serde_json::from_slice(&bytes)
                .map_err(|_| "pull request response is not complete JSON")?;
            self.scan(&value)?;
            Ok((value, linked))
        };
        let mut cancellation = control.clone();
        tokio::select! {
            result = work => result,
            _ = cancellation.stopped() => Err("pull request transport interrupted; original effect requires reconciliation".into()),
        }
    }

    pub(super) async fn preflight(&self, intent: &Intent, control: &Control) -> Result<(), String> {
        pr::validate_intent(intent, &self.bounds)?;
        if intent.head.target != self.target { return Err("pull request target differs from accepted intent".into()); }
        let (repository, _) = self.exchange(Method::GET, self.endpoint(&[])?, None, StatusCode::OK, control).await?;
        if repository.get("id").and_then(|v| v.as_u64()) != Some(self.target.repository_id)
            || repository.get("name").and_then(|v| v.as_str()) != Some(self.target.repository.as_str())
            || repository.pointer("/owner/login").and_then(|v| v.as_str()) != Some(self.target.owner.as_str()) {
            return Err("pull request provider repository identity differs".into());
        }
        let head = intent.head.head_ref.strip_prefix("refs/heads/").ok_or("pull request head is not a branch")?;
        for (branch, exact) in [(head, Some(intent.head.binding.checkpoint.commit.as_str())), (self.target.base_branch.as_str(), None)] {
            let mut suffix = vec!["git", "ref", "heads"];
            suffix.extend(branch.split('/'));
            let (value, _) = self.exchange(Method::GET, self.endpoint(&suffix)?, None, StatusCode::OK, control).await?;
            if value.get("ref").and_then(|v| v.as_str()) != Some(format!("refs/heads/{branch}").as_str())
                || value.pointer("/object/type").and_then(|v| v.as_str()) != Some("commit") {
                return Err("pull request provider branch identity differs".into());
            }
            let commit = value.pointer("/object/sha").and_then(|v| v.as_str()).ok_or("pull request provider branch commit missing")?;
            if !git::valid_commit(commit) || exact.is_some_and(|expected| expected != commit) {
                return Err("pull request provider head differs from exact accepted checkpoint".into());
            }
        }
        Ok(())
    }

    /// Exactly one POST. Caller must have durably marked this original intent
    /// as possibly sent. No status or transport failure here permits repetition.
    pub(super) async fn create(&self, intent: &Intent, control: &Control) -> Result<Proof, String> {
        if intent.head.target != self.target { return Err("pull request target differs from accepted intent".into()); }
        let body = pr::create_body(intent, &self.bounds)?;
        let (value, _) = self.exchange(Method::POST, self.endpoint(&["pulls"])?, Some(body), StatusCode::CREATED, control).await?;
        let proof = self.proof(intent, value)?;
        pr::validate_proof(intent, &proof, &self.bounds)?;
        Ok(proof)
    }

    /// One original-identity page, all states; truncation and ambiguity are
    /// unknown. The method contains no create path and never follows Link.
    pub(super) async fn reconcile(&self, intent: &Intent, control: &Control) -> Result<pr::Reconciliation, String> {
        pr::validate_intent(intent, &self.bounds)?;
        if intent.head.target != self.target { return Err("pull request target differs from accepted intent".into()); }
        let head = intent.head.head_ref.strip_prefix("refs/heads/").ok_or("pull request head is not a branch")?;
        let per_page = self.bounds.lookup_rows.min(100); // GitHub's protocol maximum, not a policy default.
        if per_page == 0 { return Err("pull request lookup row allowance is zero".into()); }
        let mut url = self.endpoint(&["pulls"])?;
        url.query_pairs_mut().append_pair("state", "all")
            .append_pair("head", &format!("{}:{head}", self.target.owner))
            .append_pair("base", &self.target.base_branch)
            .append_pair("per_page", &per_page.to_string()).append_pair("page", "1");
        let (value, linked) = self.exchange(Method::GET, url, None, StatusCode::OK, control).await?;
        let values = value.as_array().ok_or("pull request lookup is not an array")?;
        if values.len() > per_page { return Err("pull request lookup exceeds requested bound".into()); }
        let mut proofs = Vec::with_capacity(values.len());
        for value in values { proofs.push(self.proof(intent, value.clone())?); }
        // GitHub omits Link when all results fit one page, including a page
        // exactly at per_page. A link of any kind keeps this one-page lookup
        // incomplete; no supplied link is followed.
        Ok(pr::reconcile(intent, &proofs, !linked, &self.bounds)?)
    }

    fn proof(&self, intent: &Intent, value: serde_json::Value) -> Result<Proof, String> {
        // GitHub objects contain unrelated extensible fields; only the bounded
        // known projection is retained and then checked by the closed contract.
        #[derive(Deserialize)] struct Owner { login: String }
        #[derive(Deserialize)] struct Repo { id: u64, name: String, owner: Owner }
        #[derive(Deserialize)] struct Branch { #[serde(rename="ref")] branch: String, sha: String, repo: Repo }
        #[derive(Deserialize)] struct Pull { id: u64, number: u64, html_url: String, title: String, body: Option<String>, head: Branch, base: Branch }
        let pull: Pull = serde_json::from_value(value).map_err(|_| "pull request provider identity is incomplete")?;
        let branch = |value: Branch| BranchProof { repository_id: value.repo.id, owner: value.repo.owner.login,
            repository: value.repo.name, branch: value.branch, commit: value.sha };
        Ok(Proof { operation_id: intent.head.binding.operation_id.clone(), provider_id: pull.id, number: pull.number,
            url: pull.html_url, head: branch(pull.head), base: branch(pull.base), title: pull.title,
            body: pull.body.unwrap_or_default(), marker: intent.marker.clone() })
    }
}

#[cfg(test)]
#[path = "pull_request_provider_tests.rs"]
mod tests;
