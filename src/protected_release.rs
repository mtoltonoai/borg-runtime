//! Restricted release producer transport. Ordinary task HTTP is not a producer.
//! Deployment account/path protection is required; same-UID fixtures prove no isolation.
use crate::{controller::Controller, limits::InterfaceLimits, protocol::ReleaseArtifact};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, path::PathBuf, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{UnixListener, UnixStream},
};
pub mod repository;

pub const REVISION: u32 = 1;
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Checker,
    Repository,
    Human,
    Rollout,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Producer {
    pub id: String,
    pub uid: u32,
    pub role: Role,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub socket: PathBuf,
    pub worker_uids: Vec<u32>,
    pub producers: Vec<Producer>,
    pub policy: String,
    pub recipe_sha256: String,
    pub required_checks: Vec<String>,
    pub max_check_age_ms: u64,
    pub repository: String,
    pub repository_path: PathBuf,
    pub target_ref: String,
    pub deployment_target: String,
    pub target_revision: String,
    pub installed_package_sha256: String,
    pub max_records: usize,
}
fn id(v: &str, limits: &InterfaceLimits) -> bool {
    !v.is_empty()
        && v.len() <= limits.protected_identity_bytes
        && v.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.:/".contains(&b))
}
pub(crate) fn hash(v: &str) -> bool {
    v.len() == 64
        && v.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
impl Config {
    pub fn validate(&self, limits: &InterfaceLimits) -> Result<(), String> {
        let ids: BTreeSet<_> = self.producers.iter().map(|p| &p.id).collect();
        let uids: BTreeSet<_> = self.producers.iter().map(|p| p.uid).collect();
        let checks: BTreeSet<_> = self.required_checks.iter().collect();
        if self.socket.as_os_str().len() > limits.protected_socket_path_bytes {
            return Err("invalid protected release configuration: interface.protected_socket_path_bytes".into());
        }
        if self.producers.iter().any(|p| p.id.len() > limits.protected_identity_bytes)
            || self.required_checks.iter().any(|v| v.len() > limits.protected_identity_bytes)
            || [&self.policy, &self.repository, &self.deployment_target, &self.target_revision, &self.target_ref]
                .iter().any(|v| v.len() > limits.protected_identity_bytes) {
            return Err("invalid protected release configuration: interface.protected_identity_bytes".into());
        }
        if !self.socket.is_absolute()
            || !self.repository_path.is_absolute()
            || self.producers.is_empty()
            || self.worker_uids.is_empty()
            || ids.len() != self.producers.len()
            || uids.len() != self.producers.len()
            || self
                .producers
                .iter()
                .any(|p| !id(&p.id, limits) || self.worker_uids.contains(&p.uid))
            || self.required_checks.is_empty()
            || checks.len() != self.required_checks.len()
            || self.required_checks.iter().any(|v| !id(v, limits))
            || !id(&self.policy, limits)
            || !id(&self.repository, limits)
            || !hash(&self.recipe_sha256)
            || !id(&self.deployment_target, limits)
            || !id(&self.target_revision, limits)
            || !hash(&self.installed_package_sha256)
            || !self.target_ref.starts_with("refs/heads/")
            || !id(&self.target_ref, limits)
            || self.target_ref.contains("..")
            || self.max_check_age_ms == 0
            || self.max_records == 0
        {
            return Err(
                "invalid protected release configuration or overlapping worker/producer identities"
                    .into(),
            );
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Subject {
    pub release_id: String,
    pub artifact: ReleaseArtifact,
    pub repository: String,
    pub target_ref: String,
    pub expected_base: String,
    pub policy: String,
    pub deployment_target: String,
    pub target_revision: String,
    pub installed_package_sha256: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Check {
    pub subject: Subject,
    pub recipe_sha256: String,
    pub checked_at_ms: u64,
    pub valid_until_ms: u64,
    pub results: std::collections::BTreeMap<String, bool>,
    pub cleanup_verified: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    Merge,
    Rollout,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum State {
    Prepared,
    Started,
    Applied,
    Refused,
    Unknown,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Action {
    pub id: String,
    pub kind: Kind,
    pub subject: Subject,
    pub check_id: String,
    pub grant_id: Option<String>,
    pub state: State,
    pub observation: Option<String>,
    pub dispatch_until_ms: Option<u64>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(
    tag = "operation",
    content = "input",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum Command {
    Check(Check),
    Grant {
        subject: Subject,
        valid_until_ms: u64,
    },
    Prepare {
        kind: Kind,
        subject: Subject,
        check_id: String,
        grant_id: Option<String>,
    },
    ExecuteMerge {
        action_id: String,
    },
    ReconcileMerge {
        action_id: String,
    },
    StartRollout {
        action_id: String,
        subject: Subject,
    },
    ObserveRollout {
        action_id: String,
        subject: Subject,
        installed_package_sha256: String,
        observed: bool,
    },
    ReconcileObservation {
        action_id: String,
        state: State,
        observation: String,
    },
    GetAction {
        action_id: String,
    },
    GetReceipt {
        operation_id: String,
    },
    View {
        release_id: String,
    },
    Start {
        action_id: String,
    },
    Finish {
        action_id: String,
        state: State,
        observation: String,
    },
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub revision: u32,
    pub operation_id: String,
    pub command: Command,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Reply {
    pub status: String,
    pub data: serde_json::Value,
    pub replayed: bool,
}
impl Reply {
    pub(crate) fn refused(code: &str) -> Self {
        Self {
            status: code.into(),
            data: serde_json::Value::Null,
            replayed: false,
        }
    }
    pub(crate) fn ok(v: impl Serialize) -> Self {
        Self {
            status: "ok".into(),
            data: serde_json::to_value(v).expect("serializable record"),
            replayed: false,
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct StageRecord {
    pub operation_id: String,
    pub producer_id: String,
    pub command: Command,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct StageView {
    pub candidate: crate::protocol::ReleaseCandidate,
    pub configured: bool,
    pub policy: Option<String>,
    pub records: Vec<StageRecord>,
    pub actions: Vec<Action>,
    pub automatic_rollout: bool,
}

pub(crate) fn request(id: String, command: Command) -> Request {
    Request {
        revision: REVISION,
        operation_id: id,
        command,
    }
}

pub fn load_config(path: &std::path::Path, limits: &InterfaceLimits) -> Result<Config, String> {
    use std::io::Read;
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    if !path.is_absolute() {
        return Err("protected config requires an absolute operator path".into());
    }
    for p in path.ancestors() {
        let m = std::fs::symlink_metadata(p).map_err(|e| e.to_string())?;
        if m.file_type().is_symlink() || m.permissions().mode() & 0o022 != 0 {
            return Err("protected config path is aliased or writable by other accounts".into());
        }
    }
    let m = std::fs::symlink_metadata(path).map_err(|e| e.to_string())?;
    if !m.is_file() || m.nlink() != 1 {
        return Err("protected config file bound/type: regular single-link file required".into());
    }
    if m.len() > limits.protected_config_bytes as u64 {
        return Err("protected config file bound/type: interface.protected_config_bytes".into());
    }
    let read_bound = u64::try_from(limits.protected_config_bytes)
        .ok()
        .and_then(|n| n.checked_add(1))
        .ok_or("protected config byte limit cannot be represented: interface.protected_config_bytes")?;
    let mut bytes = Vec::new();
    std::fs::File::open(path)
        .map_err(|e| e.to_string())?
        .take(read_bound)
        .read_to_end(&mut bytes)
        .map_err(|e| e.to_string())?;
    if bytes.len() > limits.protected_config_bytes {
        return Err("protected config file bound/type: interface.protected_config_bytes".into());
    }
    let cfg: Config = serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;
    cfg.validate(limits)?;
    for p in path.ancestors() {
        let m = std::fs::symlink_metadata(p).map_err(|e| e.to_string())?;
        if cfg.worker_uids.contains(&m.uid()) {
            return Err("worker-owned protected configuration path refused".into());
        }
    }
    Ok(cfg)
}

/// Bind only an operator-created protected directory; never unlink a stale socket.
/// Mode/UID checks detect mistakes, not malicious same-UID administrators or ACLs.
pub fn listen(config: &Config, limits: &InterfaceLimits) -> Result<UnixListener, String> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    config.validate(limits)?;
    let parent = config.socket.parent().ok_or("missing socket parent")?;
    let m = std::fs::symlink_metadata(parent).map_err(|e| e.to_string())?;
    if !m.is_dir()
        || m.file_type().is_symlink()
        || m.permissions().mode() & 0o022 != 0
        || config.worker_uids.contains(&m.uid())
        || config.socket.exists()
        || parent.ancestors().any(|p| {
            std::fs::symlink_metadata(p)
                .map(|m| m.file_type().is_symlink())
                .unwrap_or(true)
        })
    {
        return Err("protected socket parent is writable by worker/other users, aliased, or socket already exists".into());
    }
    let listener = UnixListener::bind(&config.socket).map_err(|e| e.to_string())?;
    std::fs::set_permissions(&config.socket, std::fs::Permissions::from_mode(0o660))
        .map_err(|e| e.to_string())?;
    Ok(listener)
}
// Preserve the private completion namespace independently of configured lengths.
fn validate_public_operation(req: &Request, limits: &InterfaceLimits) -> Result<(), String> {
    if req.revision != REVISION || req.operation_id.is_empty()
        || !req.operation_id.bytes().all(|b| b.is_ascii_alphanumeric() || b"-_.:/".contains(&b))
        || req.operation_id.starts_with("finish:") {
        return Err("protected public operation identity bound/namespace".into());
    }
    if req.operation_id.len() > limits.protected_identity_bytes {
        return Err("protected public operation identity bound/namespace: interface.protected_identity_bytes".into());
    }
    if req.operation_id.len() > limits.protected_operation_id_bytes {
        return Err("protected public operation identity bound/namespace: interface.protected_operation_id_bytes".into());
    }
    Ok(())
}
async fn exchange(
    controller: &Controller,
    cfg: &Config,
    mut stream: UnixStream,
) -> Result<(), String> {
    let limits = &controller.limits().interface;
    let uid = stream.peer_cred().map_err(|e| e.to_string())?.uid();
    if !cfg.producers.iter().any(|p| p.uid == uid) {
        return Err("unauthenticated protected producer".into());
    }
    let n = stream.read_u32().await.map_err(|e| e.to_string())? as usize;
    if n == 0 { return Err("protected request bound: empty request frame".into()); }
    if n > limits.protected_request_bytes {
        return Err("protected request bound: interface.protected_request_bytes".into());
    }
    let mut bytes = vec![0; n];
    stream
        .read_exact(&mut bytes)
        .await
        .map_err(|e| e.to_string())?;
    let req: Request = serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;
    validate_public_operation(&req, limits)?;
    if matches!(
        req.command,
        Command::Start { .. } | Command::Finish { .. } | Command::ReconcileObservation { .. }
    ) {
        return Err("internal action operation is not a producer request".into());
    }
    let result = match &req.command {
        Command::ExecuteMerge { action_id } => {
            let started = controller.protected(uid, req.clone()).await;
            if started.status != "ok" || started.replayed {
                started
            } else {
                let action: Action =
                    serde_json::from_value(started.data.clone()).map_err(|e| e.to_string())?;
                let (state, observation) = repository::execute(cfg, &action, limits).await;
                let _ = controller
                    .protected(
                        uid,
                        request(
                            format!("finish:{}", req.operation_id),
                            Command::Finish {
                                action_id: action_id.clone(),
                                state,
                                observation,
                            },
                        ),
                    )
                    .await;
                // This public identity acknowledges its original durable admission.
                // Completion is read from GetAction, never substituted on replay.
                started
            }
        }
        Command::ReconcileMerge { action_id } => {
            let old = controller
                .protected(
                    uid,
                    request(
                        req.operation_id.clone(),
                        Command::GetReceipt {
                            operation_id: req.operation_id.clone(),
                        },
                    ),
                )
                .await;
            if old.status == "ok" {
                let producer: Producer = serde_json::from_value(old.data["producer"].clone())
                    .map_err(|e| e.to_string())?;
                let original: Request = serde_json::from_value(old.data["request"].clone())
                    .map_err(|e| e.to_string())?;
                if producer.uid != uid || original != req {
                    Reply::refused("operation_conflict")
                } else {
                    let mut reply: Reply = serde_json::from_value(old.data["reply"].clone())
                        .map_err(|e| e.to_string())?;
                    reply.replayed = true;
                    reply
                }
            } else if old.status != "not_found" {
                old
            } else {
                let found = controller
                    .protected(
                        uid,
                        request(
                            req.operation_id.clone(),
                            Command::GetAction {
                                action_id: action_id.clone(),
                            },
                        ),
                    )
                    .await;
                if found.status != "ok" {
                    found
                } else {
                    let action: Action =
                        serde_json::from_value(found.data.clone()).map_err(|e| e.to_string())?;
                    let (state, observation) = if action.kind == Kind::Merge
                        && matches!(action.state, State::Started | State::Unknown)
                    {
                        repository::observe(cfg, &action, limits).await
                    } else {
                        (
                            action.state.clone(),
                            action.observation.clone().unwrap_or_else(|| {
                                "action not eligible for repository readback".into()
                            }),
                        )
                    };
                    controller
                        .protected(
                            uid,
                            request(
                                req.operation_id.clone(),
                                Command::ReconcileObservation {
                                    action_id: action_id.clone(),
                                    state,
                                    observation,
                                },
                            ),
                        )
                        .await
                }
            }
        }
        _ => controller.protected(uid, req).await,
    };
    let out = serde_json::to_vec(&result).map_err(|e| e.to_string())?;
    if out.len() > limits.protected_reply_bytes {
        return Err("protected reply bound: interface.protected_reply_bytes".into());
    }
    let out_len = u32::try_from(out.len()).map_err(|_| "protected reply frame width")?;
    stream.write_u32(out_len).await.map_err(|e| e.to_string())?;
    stream.write_all(&out).await.map_err(|e| e.to_string())?;
    Ok(())
}
pub async fn serve(
    controller: Controller,
    cfg: Config,
    listener: UnixListener,
) -> Result<(), String> {
    let slots = std::sync::Arc::new(tokio::sync::Semaphore::new(
        controller.limits().interface.protected_connections,
    ));
    loop {
        let (stream, _) = listener.accept().await.map_err(|e| e.to_string())?;
        let Ok(permit) = slots.clone().try_acquire_owned() else {
            drop(stream);
            continue;
        };
        let ctl = controller.clone();
        let c = cfg.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let timeout =
                Duration::from_millis(ctl.limits().interface.protected_exchange_timeout_ms);
            let _ = tokio::time::timeout(timeout, exchange(&ctl, &c, stream)).await;
        });
    }
}
pub async fn call(
    socket: &std::path::Path,
    req: &Request,
    limits: &InterfaceLimits,
) -> Result<Reply, String> {
    let bytes = serde_json::to_vec(req).map_err(|e| e.to_string())?;
    if bytes.len() > limits.protected_request_bytes {
        return Err("request bound: interface.protected_request_bytes".into());
    }
    let request_len = u32::try_from(bytes.len()).map_err(|_| "protected request frame width")?;
    tokio::time::timeout(
        Duration::from_millis(limits.protected_call_timeout_ms),
        async {
            let mut s = UnixStream::connect(socket)
                .await
                .map_err(|e| e.to_string())?;
            s.write_u32(request_len).await.map_err(|e| e.to_string())?;
            s.write_all(&bytes).await.map_err(|e| e.to_string())?;
            let n = s.read_u32().await.map_err(|e| e.to_string())? as usize;
            if n == 0 { return Err("reply bound: empty response frame".into()); }
            if n > limits.protected_reply_bytes {
                return Err("reply bound: interface.protected_reply_bytes".into());
            }
            let mut out = vec![0; n];
            s.read_exact(&mut out).await.map_err(|e| e.to_string())?;
            serde_json::from_slice(&out).map_err(|e| e.to_string())
        },
    )
    .await
    .map_err(|_| "protected exchange timeout; reconcile original identity: interface.protected_call_timeout_ms".to_string())?
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new() -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let path = std::env::temp_dir().join(format!(
                "borg-protected-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir(&path).unwrap();
            Self(path)
        }

        fn path(&self) -> &std::path::Path {
            &self.0
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn config() -> Config {
        Config {
            socket: PathBuf::from("/run/borg/protected.sock"),
            worker_uids: vec![1001],
            producers: vec![Producer {
                id: "checker".into(),
                uid: 1002,
                role: Role::Checker,
            }],
            policy: "release".into(),
            recipe_sha256: "a".repeat(64),
            required_checks: vec!["build".into()],
            max_check_age_ms: 86_400_001,
            repository: "repo".into(),
            repository_path: PathBuf::from("/srv/borg/repo.git"),
            target_ref: "refs/heads/main".into(),
            deployment_target: "local".into(),
            target_revision: "revision".into(),
            installed_package_sha256: "b".repeat(64),
            max_records: 1_000_001,
        }
    }

    #[test]
    fn configuration_uses_selected_bounds_without_implicit_age_or_record_caps() {
        let mut limits = crate::limits::RuntimeLimits::seed().unwrap().interface;
        let mut cfg = config();
        cfg.policy = "p".repeat(limits.protected_identity_bytes + 1);
        assert_eq!(cfg.validate(&limits).unwrap_err(), "invalid protected release configuration: interface.protected_identity_bytes");
        limits.protected_identity_bytes = cfg.policy.len();
        assert!(cfg.validate(&limits).is_ok());
        limits.protected_socket_path_bytes = cfg.socket.as_os_str().len() - 1;
        assert_eq!(cfg.validate(&limits).unwrap_err(), "invalid protected release configuration: interface.protected_socket_path_bytes");
        limits.protected_socket_path_bytes += 1;
        assert!(cfg.validate(&limits).is_ok());
        cfg.max_check_age_ms = 0;
        assert!(cfg.validate(&limits).is_err());
        cfg.max_check_age_ms = 1;
        cfg.max_records = 0;
        assert!(cfg.validate(&limits).is_err());
    }

    #[test]
    fn public_identity_reports_selected_limit_without_relaxing_private_namespace() {
        let mut limits = crate::limits::RuntimeLimits::seed().unwrap().interface;
        limits.protected_identity_bytes = 5;
        limits.protected_operation_id_bytes = 4;
        let mut req = request("abcdef".into(), Command::GetAction { action_id: "action".into() });
        assert_eq!(validate_public_operation(&req, &limits).unwrap_err(),
            "protected public operation identity bound/namespace: interface.protected_identity_bytes");
        req.operation_id = "abcde".into();
        assert_eq!(validate_public_operation(&req, &limits).unwrap_err(),
            "protected public operation identity bound/namespace: interface.protected_operation_id_bytes");
        req.operation_id = "abcd".into();
        assert!(validate_public_operation(&req, &limits).is_ok());
        req.operation_id = "finish:abcd".into();
        assert_eq!(validate_public_operation(&req, &limits).unwrap_err(),
            "protected public operation identity bound/namespace");
        req.operation_id = "a\n".into();
        assert_eq!(validate_public_operation(&req, &limits).unwrap_err(),
            "protected public operation identity bound/namespace");
    }

    #[tokio::test]
    async fn client_honors_exact_request_and_reply_byte_bounds() {
        let dir = TestDirectory::new();
        let socket = dir.path().join("protected.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let req = request(
            "read".into(),
            Command::GetAction {
                action_id: "action".into(),
            },
        );
        let request_bytes = serde_json::to_vec(&req).unwrap();
        let reply = Reply::ok(serde_json::json!({ "action": "observed" }));
        let reply_bytes = serde_json::to_vec(&reply).unwrap();
        let mut limits = crate::limits::RuntimeLimits::seed().unwrap().interface;
        limits.protected_request_bytes = request_bytes.len() - 1;
        assert_eq!(
            call(&socket, &req, &limits).await.unwrap_err(),
            "request bound: interface.protected_request_bytes"
        );
        limits.protected_request_bytes = request_bytes.len();
        limits.protected_reply_bytes = reply_bytes.len();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let length = stream.read_u32().await.unwrap() as usize;
            let mut actual = vec![0; length];
            stream.read_exact(&mut actual).await.unwrap();
            assert_eq!(actual, request_bytes);
            stream.write_u32(reply_bytes.len() as u32).await.unwrap();
            stream.write_all(&reply_bytes).await.unwrap();
        });
        assert_eq!(call(&socket, &req, &limits).await.unwrap(), reply);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn client_refuses_reply_length_before_reading_oversized_payload() {
        let dir = TestDirectory::new();
        let socket = dir.path().join("protected.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let req = request(
            "read".into(),
            Command::GetAction {
                action_id: "action".into(),
            },
        );
        let mut limits = crate::limits::RuntimeLimits::seed().unwrap().interface;
        limits.protected_reply_bytes = 1;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let length = stream.read_u32().await.unwrap() as usize;
            let mut actual = vec![0; length];
            stream.read_exact(&mut actual).await.unwrap();
            stream.write_u32(2).await.unwrap();
        });
        assert_eq!(
            call(&socket, &req, &limits).await.unwrap_err(),
            "reply bound: interface.protected_reply_bytes"
        );
        server.await.unwrap();
    }
}

// Only these fixed metadata-bound diagnostics enter protected error data. No
// candidate values, arbitrary error text or authority claims are projected.
const LIMIT_REFUSAL_FRAMES: &[(&str, &str, &str)] = &[
    ("response_bound", "stage records exceed bound: interface.protected_reply_bytes", "interface.protected_reply_bytes"),
    ("response_bound", "stage actions exceed bound: interface.protected_reply_bytes", "interface.protected_reply_bytes"),
    ("response_bound", "stage view exceeds bound: interface.protected_reply_bytes", "interface.protected_reply_bytes"),
    ("response_bound", "action view exceeds bound: interface.protected_reply_bytes", "interface.protected_reply_bytes"),
    ("response_bound", "protected view exceeds bound: interface.protected_reply_bytes", "interface.protected_reply_bytes"),
    ("response_bound", "original receipt lookup envelope exceeds bound: interface.protected_reply_bytes", "interface.protected_reply_bytes"),
    ("invalid_check", "bounded configured check interval/recipe required: protected_release.max_check_age_ms", "protected_release.max_check_age_ms"),
    ("invalid_check", "bounded configured check interval/recipe required: controller.protected_check_results", "controller.protected_check_results"),
    ("invalid_grant", "bounded human grant interval required: protected_release.max_check_age_ms", "protected_release.max_check_age_ms"),
    ("invalid_observation", "invalid original action observation: controller.protected_observation_bytes", "controller.protected_observation_bytes"),
    ("invalid_observation", "invalid original action readback: controller.protected_observation_bytes", "controller.protected_observation_bytes"),
    ("invalid_input", "revision, identity or body bound: interface.protected_request_bytes", "interface.protected_request_bytes"),
    ("invalid_input", "revision, identity or body bound: transport.identity_bytes", "transport.identity_bytes"),
    ("capacity", "protected receipt capacity exhausted: protected_release.max_records", "protected_release.max_records"),
    ("capacity", "protected action settlement capacity is reserved: protected_release.max_records", "protected_release.max_records"),
    ("capacity_or_unavailable", "controller request queue is full: controller.queue_capacity", "controller.queue_capacity"),

];
pub(crate) fn refusal_with_limit(code: &str, message: &str) -> Reply {
    let mut reply = Reply::refused(code);
    if let Some((_, fixed_message, field)) = LIMIT_REFUSAL_FRAMES.iter().find(|(c, m, _)| *c == code && *m == message) {
        reply.data = serde_json::json!({"limiting_field":field,"message":fixed_message});
    }
    reply
}
pub(crate) fn minimum_limit_refusal_bytes() -> Result<usize, String> {
    LIMIT_REFUSAL_FRAMES.iter().map(|(code, message, _)| {
        serde_json::to_vec(&refusal_with_limit(code, message)).map(|value| value.len()).map_err(|e| e.to_string())
    }).try_fold(0, |largest, size| size.map(|size| largest.max(size)))
}
