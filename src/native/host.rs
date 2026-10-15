//! Host-owned process accounting. These paths are never task/provider inputs.
use crate::limits::{NativeLimits, WorkerLimits};
use std::{fs::File, path::PathBuf, sync::Arc};

pub struct ValidationRuntime {
    pub(super) worker_limits: Arc<WorkerLimits>,
    pub(super) slots: Arc<tokio::sync::Semaphore>,
    pub(super) request_process: super::request_observation::Process,
}
impl ValidationRuntime {
    pub fn new(worker_limits: Arc<WorkerLimits>) -> Result<Self, String> {
        worker_limits.validate()?;
        Ok(Self {
            slots: Arc::new(tokio::sync::Semaphore::new(
                worker_limits.native_validation_capacity,
            )),
            request_process: super::request_observation::Process::new(&worker_limits),
            worker_limits,
        })
    }

    /// Host-triggered best-effort diagnostic only. Does not poll, cancel or
    /// recreate any provider/tool future, reserve usage, or mutate task state.
    pub(crate) fn probe_request_clock(&self) {
        self.request_process.probe_clock();
    }
}

/// The owner must recover this durable registry under its exclusive worker or
/// database ownership before admitting work. A path alone does not prove that
/// recovery occurred, identify an attempt, or settle a provider outcome.
#[derive(Clone)]
pub struct ValidationHost {
    pub native_limits: Arc<NativeLimits>,
    pub(super) runtime: Arc<ValidationRuntime>,
    pub program: PathBuf,
    pub registry: PathBuf,
    owner_lock: Option<Arc<File>>,
    pub(super) request_correlation: Option<super::request_observation::Correlation>,
    #[cfg(test)]
    pub(super) request_capture: Option<Arc<super::request_observation::Sink>>,
    #[cfg(debug_assertions)]
    fixture_registry: Option<Arc<FixtureRegistry>>,
}
impl ValidationHost {
    pub fn new(
        program: PathBuf,
        registry: PathBuf,
        native_limits: Arc<NativeLimits>,
        runtime: Arc<ValidationRuntime>,
    ) -> Self {
        Self {
            native_limits,
            runtime,
            program,
            registry,
            owner_lock: None,
            request_correlation: None,
            #[cfg(test)]
            request_capture: None,
            #[cfg(debug_assertions)]
            fixture_registry: None,
        }
    }
    /// The controller supplies an already acquired exclusive lock. Native never
    /// acquires or authenticates it; it extends ownership through CPU cleanup.
    pub fn with_owner_lock(
        program: PathBuf,
        registry: PathBuf,
        owner_lock: Arc<File>,
        native_limits: Arc<NativeLimits>,
        runtime: Arc<ValidationRuntime>,
    ) -> Self {
        Self {
            native_limits,
            runtime,
            program,
            registry,
            owner_lock: Some(owner_lock),
            request_correlation: None,
            #[cfg(test)]
            request_capture: None,
            #[cfg(debug_assertions)]
            fixture_registry: None,
        }
    }
    pub fn with_limits(mut self, limits: Arc<NativeLimits>) -> Self {
        self.native_limits = limits;
        self
    }
    pub fn with_runtime(mut self, runtime: Arc<ValidationRuntime>) -> Self {
        self.runtime = runtime;
        self
    }
    /// Diagnostic correlation only, supplied by the worker's current attempt.
    /// Invalid/non-generated IDs omit correlation; they cannot affect execution.
    pub(crate) fn with_request_attempt(mut self, attempt: &crate::protocol::Attempt) -> Self {
        self.request_correlation = super::request_observation::Correlation::from_attempt(attempt);
        self
    }
    pub(super) fn retain_owner_lock(&self) -> Option<Arc<File>> {
        self.owner_lock.clone()
    }
    #[cfg(debug_assertions)]
    pub(super) fn retain_fixture_registry(&self) -> Option<Arc<FixtureRegistry>> {
        self.fixture_registry.clone()
    }
    pub(super) fn check(&self) -> Result<(), String> {
        self.native_limits.validate()?;
        self.runtime.worker_limits.validate()?;
        if !self.program.is_absolute() || !self.registry.is_absolute() {
            return Err("native_validation_host_invalid".into());
        }
        if !self.program.is_file() {
            return Err("native_validation_unavailable".into());
        }
        Ok(())
    }
}

/// Compatibility fixtures only. Production must supply a durable ValidationHost
/// and its startup recovery. This unique path is not a remembered restart root.
#[cfg(debug_assertions)]
pub(super) struct FixtureHost(pub ValidationHost);
#[cfg(debug_assertions)]
pub(super) struct FixtureRegistry(PathBuf);
#[cfg(debug_assertions)]
impl FixtureHost {
    pub(super) fn new(program: &std::path::Path) -> Result<Self, String> {
        use std::{
            os::unix::fs::DirBuilderExt,
            sync::atomic::{AtomicU64, Ordering},
        };
        static NEXT: AtomicU64 = AtomicU64::new(0);
        for _ in 0..8 {
            let stamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|_| "native_validation_unavailable")?
                .as_nanos();
            let registry = std::env::temp_dir().join(format!(
                "borg-native-fixture-{}-{stamp}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed),
            ));
            // PID namespaces recycle IDs. Atomically claim a fresh timestamped
            // path, retry only collisions, never replace/remove old evidence.
            match std::fs::DirBuilder::new().mode(0o700).create(&registry) {
                Ok(()) => {
                    let limits = crate::limits::RuntimeLimits::seed()?;
                    let mut host = ValidationHost::new(
                        program.to_owned(),
                        registry.clone(),
                        Arc::new(limits.execution.native),
                        Arc::new(ValidationRuntime::new(Arc::new(limits.worker))?),
                    );
                    host.fixture_registry = Some(Arc::new(FixtureRegistry(registry)));
                    return Ok(Self(host));
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(_) => return Err("native_validation_unavailable".into()),
            }
        }
        Err("native_validation_unavailable".into())
    }
}
#[cfg(debug_assertions)]
impl Drop for FixtureRegistry {
    fn drop(&mut self) {
        // An unverified process record must survive this fixture owner. Remove
        // only an empty directory, never recursively erase recovery evidence.
        let _ = std::fs::remove_dir(&self.0);
    }
}
