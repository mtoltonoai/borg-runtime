//! Optional requested GitHub publication: two original durable effects.
//!
//! This module validates values; it performs no I/O and grants no authority.
//! Callers supply accepted immutable configuration, canonical task/checkpoint
//! and operation records, and enforce ownership/deadline before any new send.
//! The Git head publication is distinct from checkpoint publication. A PR may
//! follow only its exact settled head effect. Unknown provider outcomes never
//! authorize a second create. There is no GitHub API idempotency-key claim.
use crate::protocol::{Attempt, Checkpoint, OperationRecord, Repository, Task};
use crate::repository_catalogue::Revision;
use serde::{Deserialize, Deserializer, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};

pub const HEAD_KIND: &str = "pull_request_head";
pub const CREATE_KIND: &str = "pull_request";
const MARKER_PREFIX: &str = "<!-- borg-pr-operation:";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Target {
    pub revision: String,
    pub api_base: String,
    pub owner: String,
    pub repository: String,
    /// Configured provider repository identity, never inferred from a mutable name.
    pub repository_id: u64,
    /// Explicit trusted association with the provider destination; never origin discovery.
    pub head_remote: String,
    /// Existing branch name, without refs/heads/; never an arbitrary commit.
    pub base_branch: String,
    /// Configuration selector only. No credential is loaded or retained here.
    pub credential_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub title: String,
    pub body: String,
}

/// Explicit caller-supplied limits. No runtime metadata values are selected here.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Bounds {
    pub identity_bytes: usize,
    pub endpoint_bytes: usize,
    pub ref_bytes: usize,
    pub title_bytes: usize,
    pub body_bytes: usize,
    pub record_bytes: usize,
    pub lookup_rows: usize,
}


impl Bounds {
    /// Projection of existing accepted metadata, not new policy defaults.
    /// Callers still preflight complete nested receipts/journals and response bytes.
    pub fn from_limits(limits: &crate::limits::RuntimeLimits) -> Self {
        Self {
            identity_bytes: limits.transport.identity_bytes,
            endpoint_bytes: limits.repository.endpoint_bytes.min(limits.controller.max_repository_identity_bytes),
            ref_bytes: limits.controller.max_checkpoint_ref_bytes,
            title_bytes: limits.controller.max_progress_bytes,
            body_bytes: limits.controller.effect_value_bytes,
            record_bytes: limits.controller.effect_value_bytes,
            lookup_rows: limits.controller.max_list_tasks,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Binding {
    /// Original PR-create identity, distinct from checkpoint and head identities.
    pub operation_id: String,
    pub task_id: String,
    pub attempt_id: String,
    pub generation: u64,
    pub registration: Revision,
    #[serde(deserialize_with = "strict_checkpoint")]
    pub checkpoint: Checkpoint,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct HeadIntent {
    pub binding: Binding,
    /// New requests retain both actors explicitly. The legacy Binding continues
    /// to describe the original source attempt, never the requesting attempt.
    #[serde(default, skip_serializing_if = "Option::is_none", deserialize_with = "present_publication")]
    pub publication: Option<Box<crate::mcp::publication::Binding>>,
    pub target: Target,
    pub head_operation_id: String,
    pub head_ref: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HeadProof {
    pub operation_id: String,
    pub target_revision: String,
    pub remote: String,
    pub remote_ref: String,
    pub commit: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HeadSettlement {
    pub acknowledged: bool,
    pub proof: HeadProof,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Intent {
    pub head: HeadIntent,
    pub head_proof: HeadProof,
    pub title: String,
    /// Exact requested body plus the one stable original-operation marker.
    pub body: String,
    pub marker: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BranchProof {
    pub repository_id: u64,
    pub owner: String,
    pub repository: String,
    /// Provider branch name, without refs/heads/.
    pub branch: String,
    pub commit: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Proof {
    pub operation_id: String,
    pub provider_id: u64,
    pub number: u64,
    pub url: String,
    pub head: BranchProof,
    pub base: BranchProof,
    pub title: String,
    pub body: String,
    pub marker: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Settlement {
    pub acknowledged: bool,
    pub proof: Proof,
}


/// Definite never-sent closure only. The caller proves its original durable phase
/// never crossed may-have-started; absent provider readback does not prove this.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NotApplied {
    pub acknowledged: bool,
    pub not_applied: bool,
    #[serde(default, skip_serializing_if = "Option::is_none", deserialize_with = "present_refusal")]
    pub refusal: Option<crate::mcp::publication::Refusal>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum HeadOutcome {
    Applied(HeadSettlement),
    NotApplied(NotApplied),
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum CreateOutcome {
    Applied(Settlement),
    NotApplied(NotApplied),
}

/// A result references original settled operations; it is not itself settlement.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ResultReference {
    pub operation_id: String,
    pub head_operation_id: String,
    pub checkpoint_operation_id: String,
    pub target_revision: String,
    pub provider_id: u64,
    pub number: u64,
    pub url: String,
    pub head: BranchProof,
    pub base: BranchProof,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Invalid(&'static str),
    Mismatch(&'static str),
    Unsettled(&'static str),
    Collision,
    Bound(&'static str),
    Decode,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Diagnostics contain closed field names, never operands or credentials.
        write!(f, "pull request validation: {self:?}")
    }
}
impl std::error::Error for Error {}
impl From<Error> for String {
    fn from(error: Error) -> Self { error.to_string() }
}
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reconciliation {
    Observed(Proof),
    /// Includes empty and incomplete observations. Never permission to resend.
    Unknown,
}

fn bounds(b: &Bounds) -> Result<()> {
    if [b.identity_bytes, b.endpoint_bytes, b.ref_bytes, b.title_bytes,
        b.body_bytes, b.record_bytes, b.lookup_rows].contains(&0) {
        return Err(Error::Invalid("bounds"));
    }
    Ok(())
}
fn text(s: &str, max: usize, empty: bool, multiline: bool, field: &'static str) -> Result<()> {
    if s.len() > max { return Err(Error::Bound(field)); }
    if (!empty && s.trim().is_empty()) || s.chars().any(|c| {
        c.is_control() && !(multiline && matches!(c, '\n' | '\r' | '\t'))
    }) { return Err(Error::Invalid(field)); }
    Ok(())
}
fn id(s: &str, b: &Bounds, field: &'static str) -> Result<()> {
    text(s, b.identity_bytes, false, false, field)
}
fn commit(s: &str) -> bool {
    matches!(s.len(), 40 | 64)
        && s.bytes().all(|v| v.is_ascii_hexdigit() && !v.is_ascii_uppercase())
}
fn component(s: &str) -> String {
    s.as_bytes().iter().map(|v| format!("{v:02x}")).collect()
}
fn provider_component(s: &str, b: &Bounds, field: &'static str) -> Result<()> {
    id(s, b, field)?;
    if matches!(s, "." | "..") || s.ends_with(".git")
        || !s.bytes().all(|v| v.is_ascii_alphanumeric() || b"._-".contains(&v)) {
        return Err(Error::Invalid(field));
    }
    Ok(())
}
fn branch(s: &str, b: &Bounds) -> Result<()> {
    text(s, b.ref_bytes, false, false, "branch")?;
    if s == "@" || s.starts_with('/') || s.ends_with('/') || s.starts_with('-')
        || s.starts_with("refs/") || s.contains("..") || s.contains("@{")
        || s.bytes().any(|v| v <= b' ' || b"~^:?*[\\".contains(&v) || v == 127)
        || s.split('/').any(|v| v.is_empty() || v.starts_with('.')
            || v.ends_with('.') || v.ends_with(".lock")) {
        return Err(Error::Invalid("branch"));
    }
    Ok(())
}
pub fn validate_branch_namespace(s: &str, b: &Bounds) -> Result<()> {
    branch(s, b)?;
    text(&format!("refs/heads/{s}/{}", "f".repeat(64)), b.ref_bytes, false, false, "branch_namespace")
}
fn endpoint(s: &str, b: &Bounds) -> Result<reqwest::Url> {
    text(s, b.endpoint_bytes, false, false, "endpoint")?;
    let u = reqwest::Url::parse(s).map_err(|_| Error::Invalid("endpoint"))?;
    let local = matches!(u.host_str(), Some("127.0.0.1" | "[::1]" | "::1"));
    if !(u.scheme() == "https" || (u.scheme() == "http" && local))
        || u.host_str().is_none() || !u.username().is_empty() || u.password().is_some()
        || u.query().is_some() || u.fragment().is_some() || s.contains('%')
        || s.contains('\\') || s.trim() != s {
        return Err(Error::Invalid("endpoint"));
    }
    Ok(u)
}
fn remote(s: &str, b: &Bounds) -> Result<()> {
    text(s, b.endpoint_bytes, false, false, "head_remote")?;
    if s.starts_with('-') || s.trim() != s || s.contains("::")
        || crate::credential::repository_contains_credentials(s) {
        return Err(Error::Invalid("head_remote"));
    }
    // Explicit URL, absolute local fixture/store path or SCP-style operand.
    // No mutable Git remote nickname, automatic fork or endpoint discovery.
    if s.starts_with('/') {
        if s.split('/').any(|c| matches!(c, "." | "..")) {
            return Err(Error::Invalid("head_remote"));
        }
    } else if s.contains("://") {
        let u = reqwest::Url::parse(s).map_err(|_| Error::Invalid("head_remote"))?;
        if !matches!(u.scheme(), "https" | "ssh") || u.host_str().is_none()
            || u.query().is_some() || u.fragment().is_some() {
            return Err(Error::Invalid("head_remote"));
        }
    } else {
        let Some((host, path)) = s.split_once(':') else {
            return Err(Error::Invalid("head_remote"));
        };
        if host.is_empty() || path.is_empty() || host.contains('/')
            || path.starts_with('-') || s.bytes().any(|v| v.is_ascii_whitespace()) {
            return Err(Error::Invalid("head_remote"));
        }
    }
    Ok(())
}

fn accepted_checkpoint_repository(s: &str, b: &Bounds) -> Result<()> {
    // This is the immutable identity of an already accepted checkpoint, not a
    // new publication destination. Preserve the existing registration endpoint
    // checks without imposing the separate Target.head_remote transport policy.
    // prepare_head and the controller still require the exact canonical source.
    text(s, b.endpoint_bytes, false, false, "checkpoint_repository")?;
    if crate::credential::repository_contains_credentials(s) {
        return Err(Error::Invalid("checkpoint_repository"));
    }
    Ok(())
}

/// Bound encoded bytes without allocating an unbounded serialization buffer.
fn record_size<T: Serialize>(v: &T, b: &Bounds) -> Result<()> {
    struct Counter { left: usize }
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > self.left { return Err(std::io::Error::other("bound")); }
            self.left -= bytes.len(); Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
    }
    bounds(b)?;
    serde_json::to_writer(Counter { left: b.record_bytes }, v)
        .map_err(|_| Error::Bound("record_bytes"))
}
fn digest<T: Serialize>(domain: &str, value: &T) -> Result<String> {
    let data = serde_json::to_vec(&(domain, value)).map_err(|_| Error::Decode)?;
    let hash = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, &data);
    Ok(hash.as_ref().iter().map(|v| format!("{v:02x}")).collect())
}
fn decode<T: DeserializeOwned>(bytes: &[u8], b: &Bounds) -> Result<T> {
    bounds(b)?;
    if bytes.len() > b.record_bytes { return Err(Error::Bound("record_bytes")); }
    serde_json::from_slice(bytes).map_err(|_| Error::Decode)
}
fn value<T: DeserializeOwned>(v: &Value, b: &Bounds) -> Result<T> {
    record_size(v, b)?;
    serde_json::from_value(v.clone()).map_err(|_| Error::Decode)
}
fn encoded<T: Serialize>(v: &T, b: &Bounds) -> Result<Value> {
    record_size(v, b)?;
    serde_json::to_value(v).map_err(|_| Error::Decode)
}

pub fn validate_target(t: &Target, b: &Bounds) -> Result<()> {
    bounds(b)?; id(&t.revision, b, "target_revision")?;
    endpoint(&t.api_base, b)?;
    if t.api_base.ends_with('/') { return Err(Error::Invalid("api_base")); }
    provider_component(&t.owner, b, "owner")?;
    provider_component(&t.repository, b, "repository")?;
    remote(&t.head_remote, b)?; branch(&t.base_branch, b)?;
    if commit(&t.base_branch) || t.repository_id == 0 {
        return Err(Error::Invalid("base_branch_or_repository_id"));
    }
    id(&t.credential_name, b, "credential_name")?;
    if !matches!(t.credential_name.as_str(), "GH_TOKEN" | "GITHUB_TOKEN") {
        return Err(Error::Invalid("credential_name"));
    }
    record_size(t, b)
}
pub fn validate_request(r: &Request, b: &Bounds) -> Result<()> {
    bounds(b)?;
    text(&r.title, b.title_bytes, false, false, "title")?;
    text(&r.body, b.body_bytes, true, true, "body")?;
    if r.body.contains(MARKER_PREFIX) { return Err(Error::Invalid("reserved_marker")); }
    record_size(r, b)
}
/// Historical absence stays checkpoint-only, even if a target is configured.
pub fn validate_selection(r: Option<&Request>, t: Option<&Target>, b: &Bounds) -> Result<()> {
    bounds(b)?;
    if let Some(r) = r {
        validate_request(r, b)?;
        validate_target(t.ok_or(Error::Invalid("target_absent"))?, b)?;
    }
    Ok(())
}

fn validate_binding(v: &Binding, b: &Bounds) -> Result<()> {
    for (s, field) in [(&v.operation_id, "operation_id"), (&v.task_id, "task_id"),
        (&v.attempt_id, "attempt_id"), (&v.registration.repository_id, "registration_id"),
        (&v.registration.registration_revision, "registration_revision"),
        (&v.checkpoint.operation_id, "checkpoint_operation_id")] { id(s, b, field)?; }
    if v.generation == 0 || v.operation_id == v.checkpoint.operation_id
        || v.registration != v.checkpoint.registration || !commit(&v.checkpoint.commit) {
        return Err(Error::Mismatch("checkpoint_binding"));
    }
    accepted_checkpoint_repository(&v.checkpoint.repository, b)?;
    let expected = format!("refs/heads/borg/tasks/{}/{}/{}", component(&v.task_id),
        component(&v.attempt_id), component(&v.checkpoint.operation_id));
    if v.checkpoint.remote_ref != expected { return Err(Error::Mismatch("checkpoint_ref")); }
    text(&v.checkpoint.remote_ref, b.ref_bytes, false, false, "checkpoint_ref")?;
    record_size(v, b)
}
fn head_identity(v: &Binding, t: &Target) -> Result<(String, String)> {
    let identity = digest("borg-pr-head", &(v, t))?;
    Ok((format!("pr-head-{identity}"), format!("refs/heads/borg/pr/{identity}")))
}
pub fn prepare_head(t: &Target, repo: &Repository, a: &Attempt, cp: &Checkpoint,
    operation_id: &str, b: &Bounds) -> Result<HeadIntent> {
    if repo.registration.as_ref() != Some(&cp.registration) || repo.checkpoint_remote != cp.repository
        || repo.pull_request_target.as_ref() != Some(t) {
        return Err(Error::Mismatch("accepted_repository"));
    }
    let binding = Binding { operation_id: operation_id.into(), task_id: a.task_id.clone(),
        attempt_id: a.id.clone(), generation: a.generation,
        registration: cp.registration.clone(), checkpoint: cp.clone() };
    validate_target(t, b)?; validate_binding(&binding, b)?;
    let (head_operation_id, head_ref) = head_identity(&binding, t)?;
    let intent = HeadIntent { binding, publication: None, target: t.clone(), head_operation_id, head_ref };
    validate_head_intent(&intent, b)?;
    Ok(intent)
}
pub fn validate_head_intent(v: &HeadIntent, b: &Bounds) -> Result<()> {
    validate_target(&v.target, b)?; validate_binding(&v.binding, b)?;
    let (operation, reference) = match &v.publication {
        None => head_identity(&v.binding, &v.target)?,
        Some(publication) => publication_head_identity(publication, &v.binding, &v.target, b)?,
    };
    if v.head_operation_id != operation || v.head_ref != reference
        || operation == v.binding.operation_id || operation == v.binding.checkpoint.operation_id {
        return Err(Error::Mismatch("head_identity"));
    }
    id(&v.head_operation_id, b, "head_operation_id")?;
    text(&v.head_ref, b.ref_bytes, false, false, "head_ref")?;
    record_size(v, b)
}
fn publication_head_identity(p: &crate::mcp::publication::Binding, legacy: &Binding, t: &Target, b: &Bounds) -> Result<(String, String)> {
    let source = &p.source;
    let caller = &p.requester.caller;
    for (value, name) in [(&p.publication_id, "publication_id"), (&p.admission_operation_id, "admission_operation_id"),
        (&p.requester.session_id, "requester_session_id"), (&source.session_id, "source_session_id"),
        (&caller.task_id, "requester_task"), (&caller.attempt_id, "requester_attempt"),
        (&caller.worker_id, "requester_worker"), (&caller.incarnation, "requester_incarnation")] { id(value, b, name)?; }
    if caller.generation == 0 || caller.task_id == source.task_id || source.task_revision == 0
        || p.publication_id != crate::mcp::publication::publication_id(&p.admission_operation_id).map_err(|_| Error::Decode)?
        || source.accepted_result.task_id != source.task_id || source.accepted_result.checkpoint.as_ref() != Some(&legacy.checkpoint)
        || source.task_id != legacy.task_id || source.checkpoint_attempt_id != legacy.attempt_id
        || source.checkpoint_generation != legacy.generation || p.selection.registration() != legacy.registration
        || source.repository.registration.as_ref() != Some(&legacy.registration)
        || source.repository.checkpoint_remote != legacy.checkpoint.repository
        || source.repository.pull_request_target.as_ref() != Some(t)
        || p.selection.target_revision() != t.revision
        || !p.selection.permits(crate::mcp::publication::Action::PublishBranch)
        || source.result_sha256.len() != 64 || !commit(&source.result_sha256) {
        return Err(Error::Mismatch("publication_source_requester"));
    }
    validate_branch_namespace(p.selection.namespace(), b)?;
    let identity = digest("borg-mcp-publication-head-v1", &(p, legacy, t))?;
    Ok((format!("pr-head-{identity}"), format!("refs/heads/{}/{identity}", p.selection.namespace())))
}
pub fn prepare_publication_head(publication: crate::mcp::publication::Binding, target: Target, b: &Bounds) -> Result<HeadIntent> {
    let source = &publication.source;
    let checkpoint = source.accepted_result.checkpoint.clone().ok_or(Error::Mismatch("checkpoint_absent"))?;
    let binding = Binding { operation_id: format!("pr-create-{}", digest("borg-mcp-publication-create-v1", &publication.publication_id)?),
        task_id: source.task_id.clone(), attempt_id: source.checkpoint_attempt_id.clone(), generation: source.checkpoint_generation,
        registration: checkpoint.registration.clone(), checkpoint };
    let (head_operation_id, head_ref) = publication_head_identity(&publication, &binding, &target, b)?;
    let head = HeadIntent { binding, publication: Some(Box::new(publication)), target, head_operation_id, head_ref };
    validate_head_intent(&head, b)?;
    Ok(head)
}
/// Source/current-owner association only. Root separately compares the accepted
/// immutable target/request and applies currentness/deadline/effect policy.
pub fn validate_head_for_task(v: &HeadIntent, task: &Task, b: &Bounds) -> Result<()> {
    let a = task.attempt.as_ref().ok_or(Error::Mismatch("attempt_absent"))?;
    let cp = task.selected_checkpoint.as_ref().ok_or(Error::Mismatch("checkpoint_absent"))?;
    let repo = task.input.repository.as_ref().ok_or(Error::Mismatch("repository_absent"))?;
    if a.task_id != task.id || a.session_id != task.session_id {
        return Err(Error::Mismatch("task_attempt"));
    }
    if &prepare_head(&v.target, repo, a, cp, &v.binding.operation_id, b)? != v {
        return Err(Error::Mismatch("task_source"));
    }
    Ok(())
}
pub fn validate_head_proof(v: &HeadIntent, p: &HeadProof, b: &Bounds) -> Result<()> {
    validate_head_intent(v, b)?;
    if p.operation_id != v.head_operation_id || p.target_revision != v.target.revision
        || p.remote != v.target.head_remote || p.remote_ref != v.head_ref
        || p.commit != v.binding.checkpoint.commit {
        return Err(Error::Mismatch("head_proof"));
    }
    record_size(p, b)
}
fn record(v: &HeadIntent, id: &str, kind: &str, intended: Value, b: &Bounds) -> Result<OperationRecord> {
    let (task_id, attempt_id, generation) = match &v.publication {
        Some(p) => (&p.requester.caller.task_id, &p.requester.caller.attempt_id, p.requester.caller.generation),
        None => (&v.binding.task_id, &v.binding.attempt_id, v.binding.generation),
    };
    let r = OperationRecord { id: id.into(), task_id: task_id.clone(),
        attempt_id: attempt_id.clone(), generation,
        kind: kind.into(), intended, state: "pending".into(), outcome: None };
    record_size(&r, b)?; Ok(r)
}
pub fn head_record(v: &HeadIntent, b: &Bounds) -> Result<OperationRecord> {
    validate_head_intent(v, b)?;
    record(v, &v.head_operation_id, HEAD_KIND, encoded(v, b)?, b)
}
fn matches_record(r: &OperationRecord, v: &HeadIntent, id: &str, kind: &str) -> Result<()> {
    let (task, attempt, generation) = match &v.publication {
        Some(p) => (&p.requester.caller.task_id, &p.requester.caller.attempt_id, p.requester.caller.generation),
        None => (&v.binding.task_id, &v.binding.attempt_id, v.binding.generation),
    };
    if r.id != id || &r.task_id != task || &r.attempt_id != attempt
        || r.generation != generation || r.kind != kind {
        return Err(Error::Mismatch("operation_record"));
    }
    if r.state != "settled" { return Err(Error::Unsettled("operation_record")); }
    Ok(())
}
fn marker(v: &Binding) -> Result<String> {
    Ok(format!("{MARKER_PREFIX}{} -->", digest("borg-pr-create", v)?))
}
fn head_marker(v: &HeadIntent) -> Result<String> {
    match &v.publication {
        None => marker(&v.binding),
        Some(p) => Ok(format!("{MARKER_PREFIX}{} -->", digest("borg-mcp-publication-create-marker-v1", &(p, &v.binding, &v.target))?)),
    }
}
fn marked_body(request: &Request, marker: &str, b: &Bounds) -> Result<String> {
    validate_request(request, b)?;
    let suffix = if request.body.is_empty() { marker.len() } else { marker.len().checked_add(2).ok_or(Error::Bound("body"))? };
    if request.body.len().checked_add(suffix).is_none_or(|n| n > b.body_bytes) {
        return Err(Error::Bound("body"));
    }
    Ok(if request.body.is_empty() { marker.into() } else { format!("{}\n\n{marker}", request.body) })
}
/// `settled` must come from the canonical original head lookup, not a reconstructed
/// caller claim. Both exact original intent and typed settlement are checked.
pub fn prepare_pull_request(head: &HeadIntent, settled: &OperationRecord,
    request: &Request, b: &Bounds) -> Result<Intent> {
    validate_head_intent(head, b)?; record_size(settled, b)?;
    matches_record(settled, head, &head.head_operation_id, HEAD_KIND)?;
    let stored: HeadIntent = value(&settled.intended, b)?;
    if &stored != head || encoded(head, b)? != settled.intended {
        return Err(Error::Mismatch("original_head_intent"));
    }
    let outcome: HeadOutcome = value(settled.outcome.as_ref().ok_or(Error::Unsettled("head_outcome"))?, b)?;
    validate_head_outcome(head, &outcome, b)?;
    let HeadOutcome::Applied(outcome) = outcome else { return Err(Error::Unsettled("head_not_applied")); };
    let marker = head_marker(head)?;
    let intent = Intent { head: head.clone(), head_proof: outcome.proof,
        title: request.title.clone(), body: marked_body(request, &marker, b)?, marker };
    validate_intent(&intent, b)?;
    Ok(intent)
}
pub fn validate_intent(v: &Intent, b: &Bounds) -> Result<()> {
    validate_head_proof(&v.head, &v.head_proof, b)?;
    text(&v.title, b.title_bytes, false, false, "title")?;
    text(&v.body, b.body_bytes, false, true, "body")?;
    if v.marker != head_marker(&v.head)? || !v.body.ends_with(&v.marker)
        || v.body.matches(MARKER_PREFIX).count() != 1
        || (v.body != v.marker && !v.body.ends_with(&format!("\n\n{}", v.marker))) {
        return Err(Error::Mismatch("original_marker"));
    }
    record_size(v, b)
}
pub fn validate_requested_result(v: &Intent, r: &Request, b: &Bounds) -> Result<()> {
    validate_intent(v, b)?;
    if v.title != r.title || v.body != marked_body(r, &v.marker, b)? {
        return Err(Error::Mismatch("accepted_request"));
    }
    Ok(())
}
pub fn intent_record(v: &Intent, b: &Bounds) -> Result<OperationRecord> {
    validate_intent(v, b)?;
    record(&v.head, &v.head.binding.operation_id, CREATE_KIND, encoded(v, b)?, b)
}
fn validate_branch(p: &BranchProof, t: &Target, b: &Bounds) -> Result<()> {
    if p.repository_id != t.repository_id || p.owner != t.owner || p.repository != t.repository || !commit(&p.commit) {
        return Err(Error::Mismatch("provider_repository"));
    }
    branch(&p.branch, b)
}
pub fn validate_proof(v: &Intent, p: &Proof, b: &Bounds) -> Result<ResultReference> {
    validate_intent(v, b)?; record_size(p, b)?;
    let t = &v.head.target;
    validate_branch(&p.head, t, b)?; validate_branch(&p.base, t, b)?;
    if p.operation_id != v.head.binding.operation_id || p.provider_id == 0 || p.number == 0
        || p.head.repository_id != p.base.repository_id
        || Some(p.head.branch.as_str()) != v.head.head_ref.strip_prefix("refs/heads/")
        || p.head.commit != v.head.binding.checkpoint.commit
        || p.base.branch != t.base_branch
        || p.title != v.title || p.body != v.body || p.marker != v.marker {
        return Err(Error::Mismatch("provider_pull_request"));
    }
    let api = endpoint(&t.api_base, b)?;
    let url = endpoint(&p.url, b)?;
    let expected_host = match api.host_str() { Some("api.github.com") => Some("github.com"), other => other };
    // Retain the provider's exact evidence, but reject alternate URL spellings.
    // The worker preflights this canonical representation before any effect.
    if url.as_str() != p.url.as_str()
        || url.scheme() != api.scheme() || url.host_str() != expected_host
        || url.port_or_known_default() != api.port_or_known_default()
        || url.path() != format!("/{}/{}/pull/{}", t.owner, t.repository, p.number) {
        return Err(Error::Mismatch("provider_url"));
    }
    let result = ResultReference { operation_id: p.operation_id.clone(),
        head_operation_id: v.head.head_operation_id.clone(),
        checkpoint_operation_id: v.head.binding.checkpoint.operation_id.clone(),
        target_revision: t.revision.clone(), provider_id: p.provider_id, number: p.number,
        url: p.url.clone(), head: p.head.clone(), base: p.base.clone() };
    record_size(&result, b)?; Ok(result)
}
pub fn reference_from_record(r: &OperationRecord, b: &Bounds) -> Result<ResultReference> {
    record_size(r, b)?;
    let intent: Intent = value(&r.intended, b)?;
    validate_intent(&intent, b)?;
    matches_record(r, &intent.head, &intent.head.binding.operation_id, CREATE_KIND)?;
    if encoded(&intent, b)? != r.intended { return Err(Error::Mismatch("original_pr_intent")); }
    let outcome: CreateOutcome = value(r.outcome.as_ref().ok_or(Error::Unsettled("pr_outcome"))?, b)?;
    validate_outcome(&intent, &outcome, b)?.ok_or(Error::Unsettled("pr_not_applied"))
}

fn validate_not_applied(v: &NotApplied, b: &Bounds) -> Result<()> {
    if !v.acknowledged || !v.not_applied { return Err(Error::Invalid("not_applied")); }
    record_size(v, b)
}
/// This checks the closed outcome value, not the caller's never-sent authority.
/// The controller must apply original pending-intent/currentness checks itself.
pub fn validate_head_outcome(v: &HeadIntent, outcome: &HeadOutcome, b: &Bounds) -> Result<()> {
    validate_head_intent(v, b)?;
    match outcome {
        HeadOutcome::Applied(s) => {
            if !s.acknowledged { return Err(Error::Unsettled("head_outcome")); }
            validate_head_proof(v, &s.proof, b)?;
        }
        HeadOutcome::NotApplied(n) => validate_not_applied(n, b)?,
    }
    record_size(outcome, b)
}
/// None is a valid negative closure, never a successful requested PR result.
pub fn validate_outcome(v: &Intent, outcome: &CreateOutcome, b: &Bounds) -> Result<Option<ResultReference>> {
    validate_intent(v, b)?;
    record_size(outcome, b)?;
    match outcome {
        CreateOutcome::Applied(s) => {
            if !s.acknowledged { return Err(Error::Unsettled("pr_outcome")); }
            Ok(Some(validate_proof(v, &s.proof, b)?))
        }
        CreateOutcome::NotApplied(n) => { validate_not_applied(n, b)?; Ok(None) }
    }
}

/// The caller proves the bounded provider observation completed. A zero-row
/// response is still Unknown after a possible send; neither branch permits POST.
pub fn reconcile(v: &Intent, observations: &[Proof], complete: bool, b: &Bounds) -> Result<Reconciliation> {
    validate_intent(v, b)?;
    if observations.len() > b.lookup_rows { return Err(Error::Bound("lookup_rows")); }
    for p in observations { validate_proof(v, p, b)?; }
    if observations.len() > 1 { return Err(Error::Collision); }
    if !complete { return Ok(Reconciliation::Unknown); }
    Ok(match observations.first() {
        Some(p) => Reconciliation::Observed(p.clone()), None => Reconciliation::Unknown,
    })
}
/// GitHub requires branch names; matching SHA evidence is checked separately.
pub fn create_body(v: &Intent, b: &Bounds) -> Result<Value> {
    validate_intent(v, b)?;
    let body = json!({"title":v.title,"body":v.body,
        "head":v.head.head_ref.strip_prefix("refs/heads/").ok_or(Error::Invalid("head_ref"))?,
        "base":v.head.target.base_branch,"maintainer_can_modify":false});
    record_size(&body, b)?; Ok(body)
}
pub fn decode_head_intent(bytes: &[u8], b: &Bounds) -> Result<HeadIntent> {
    let v = decode(bytes, b)?; validate_head_intent(&v, b)?; Ok(v)
}
pub fn decode_intent(bytes: &[u8], b: &Bounds) -> Result<Intent> {
    let v = decode(bytes, b)?; validate_intent(&v, b)?; Ok(v)
}
pub fn decode_proof(bytes: &[u8], b: &Bounds) -> Result<Proof> {
    // Shape and encoded bound only. Exact source validation needs the original intent.
    decode(bytes, b)
}

// Current protocol::Checkpoint predates deny_unknown_fields. This leaf closes
// that nested boundary without changing shared/historical checkpoint decoding.
pub(crate) fn strict_checkpoint<'de, D: Deserializer<'de>>(d: D) -> std::result::Result<Checkpoint, D::Error> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Fields { registration: Revision, repository: String, commit: String,
        remote_ref: String, operation_id: String }
    let f = Fields::deserialize(d)?;
    Ok(Checkpoint { registration: f.registration, repository: f.repository,
        commit: f.commit, remote_ref: f.remote_ref, operation_id: f.operation_id })
}
fn present_publication<'de, D: Deserializer<'de>>(d: D) -> std::result::Result<Option<Box<crate::mcp::publication::Binding>>, D::Error> {
    Box::<crate::mcp::publication::Binding>::deserialize(d).map(Some)
}
fn present_refusal<'de, D: Deserializer<'de>>(d: D) -> std::result::Result<Option<crate::mcp::publication::Refusal>, D::Error> {
    crate::mcp::publication::Refusal::deserialize(d).map(Some)
}

#[cfg(test)]
mod tests;
