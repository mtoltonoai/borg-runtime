use borg_runtime::{
    client::ApiClient,
    controller::Controller,
    protocol::Operation,
    server,
    worker::{self, WorkerConfig},
};
use std::{collections::BTreeMap, path::PathBuf, sync::Arc};

fn options(args: impl Iterator<Item = String>) -> Result<BTreeMap<String, String>, String> {
    let mut args = args;
    let mut result = BTreeMap::new();
    while let Some(key) = args.next() {
        if !key.starts_with("--") {
            return Err(format!("expected option, got {key}"));
        }
        let value = args
            .next()
            .ok_or_else(|| format!("missing value for {key}"))?;
        if result.insert(key.clone(), value).is_some() {
            return Err(format!("duplicate option {key}"));
        }
    }
    Ok(result)
}
fn required(args: &mut BTreeMap<String, String>, key: &str) -> Result<String, String> {
    args.remove(key)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| format!("required option {key}"))
}
fn number(args: &mut BTreeMap<String, String>, key: &str, default: usize) -> Result<usize, String> {
    args.remove(key)
        .map(|s| {
            s.parse::<usize>()
                .map_err(|_| format!("invalid number for {key}"))
        })
        .unwrap_or(Ok(default))
}
fn done(args: BTreeMap<String, String>) -> Result<(), String> {
    if args.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "unknown options: {:?}",
            args.keys().collect::<Vec<_>>()
        ))
    }
}

fn main() {
    if std::env::args().nth(1).as_deref() == Some("__collaboration_mcp") {
        let parsed = (|| -> Result<(), String> {
            let mut args = options(std::env::args().skip(2))?;
            let config = required(&mut args, "--config")?;
            let config_bytes = required(&mut args, "--config-bytes")?
                .parse::<usize>()
                .map_err(|_| "invalid config byte bound")?;
            done(args)?;
            worker::codex_mcp::run_stdio_config(std::path::Path::new(&config), config_bytes)
        })();
        if parsed.is_err() {
            eprintln!("borg collaboration bridge stopped");
            std::process::exit(1);
        }
        return;
    }
    // CPU validation runs in a bounded child before creating an async runtime.
    // This internal stdin protocol is never selected by a task or HTTP route.
    if std::env::args().nth(1).as_deref() == Some("__native_validate") {
        if let Err(e) = borg_runtime::native::validation_worker_stdio() {
            eprintln!("borg validation: {e}");
            std::process::exit(1);
        }
        return;
    }
    let prepared = (|| -> Result<_, String> {
        let mut argv = std::env::args().skip(1);
        let mode = argv
            .next()
            .ok_or("usage: borg-runtime controller|worker|list [options]")?;
        let mut args = options(argv)?;
        let limits = match args.remove("--limits") {
            Some(path) => borg_runtime::limits::RuntimeLimits::load(std::path::Path::new(&path))?,
            None => borg_runtime::limits::RuntimeLimits::seed()?,
        };
        Ok((mode, args, Arc::new(limits)))
    })();
    let (mode, args, limits) = match prepared {
        Ok(v) => v,
        Err(e) => {
            eprintln!("borg: {e}");
            std::process::exit(1)
        }
    };
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder
        .enable_all()
        .max_blocking_threads(limits.runtime.blocking_threads);
    if let Some(n) = limits.runtime.worker_threads {
        builder.worker_threads(n);
    }
    if let Some(n) = limits.runtime.thread_stack_bytes {
        builder.thread_stack_size(n);
    }
    let runtime = builder.build().expect("create Borg async runtime");
    if let Err(e) = runtime.block_on(run(mode, args, limits)) {
        eprintln!("borg: {e}");
        std::process::exit(1);
    }
}
async fn run(
    mode: String,
    mut args: BTreeMap<String, String>,
    limits: Arc<borg_runtime::limits::RuntimeLimits>,
) -> Result<(), String> {
    match mode.as_str() {
        "controller" => {
            let host_profile = args.remove("--host-observation-profile")
                .map(|path| borg_runtime::machine_resources::Profile::load(std::path::Path::new(&path), limits.transport.body_bytes))
                .transpose()?;
            let db = required(&mut args, "--db")?;
            let address = required(&mut args, "--listen")?
                .parse()
                .map_err(|_| "invalid listen address")?;
            let origin = required(&mut args, "--origin")?;
            let protected = args
                .remove("--protected-release-config")
                .map(
                    |path| -> Result<borg_runtime::protected_release::Config, String> {
                        borg_runtime::protected_release::load_config(
                            std::path::Path::new(&path),
                            &limits.interface,
                        )
                    },
                )
                .transpose()?;
            let repositories = args
                .remove("--repository-store-root")
                .map(|path| {
                    borg_runtime::repository_config::root(
                        std::path::Path::new(&path),
                        &limits.repository,
                    )
                })
                .transpose()?;
            done(args)?;
            let mut config = borg_runtime::controller::ControllerConfig::default();
            config.limits = limits.clone();
            config.machine_resources = host_profile.map(borg_runtime::controller::machine_resources::Config::new).transpose()?;
            config.protected_release = protected.clone();
            if let Some(repositories) = repositories {
                config.repository_read = repositories;
            }
            // Install the signal before starting the blocking database owner.
            // Dropping a JoinHandle does not cancel that owner; cancellation
            // wakes its retained SQL phase and we await actual startup exit.
            let mut shutdown = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
                .map_err(|e| e.to_string())?;
            let cancellation = borg_runtime::controller::StartupCancellation::default();
            let startup_cancel = cancellation.clone();
            let mut opening = tokio::task::spawn_blocking(move ||
                Controller::open_with_config_cancellable(db, config, startup_cancel));
            let controller = tokio::select! {
                biased;
                _ = shutdown.recv() => {
                    cancellation.cancel();
                    // A startup that committed concurrently is still closed by
                    // dropping its returned Controller; no server is published.
                    let result = opening.await.map_err(|e| e.to_string())?;
                    drop(result);
                    tokio::task::spawn_blocking(move || cancellation.wait_for_owners())
                        .await.map_err(|e| e.to_string())?;
                    return Ok(());
                }
                result = &mut opening => result.map_err(|e| e.to_string())??,
            };
            let private = match protected {
                Some(cfg) => {
                    let listener =
                        borg_runtime::protected_release::listen(&cfg, &limits.interface)?;
                    Some((cfg, listener))
                }
                None => None,
            };
            let listener = server::listen(address).await?;
            let observations = controller.start_machine_resources()?;
            println!(
                "Controller listening on {}",
                listener.local_addr().map_err(|e| e.to_string())?
            );
            let protected_controller = controller.clone();
            let protected_server = async move {
                match private {
                    Some((cfg, socket)) => {
                        borg_runtime::protected_release::serve(protected_controller, cfg, socket)
                            .await
                    }
                    None => std::future::pending().await,
                }
            };
            let result = tokio::select! {biased; _=shutdown.recv()=>Ok(()),r=server::serve(controller,listener,origin)=>r,r=protected_server=>r};
            if let Some(observations) = observations { observations.shutdown().await?; }
            result
        }
        "worker" => {
            let host_profile = args.remove("--host-observation-profile")
                .map(|path| borg_runtime::machine_resources::Profile::load(std::path::Path::new(&path), limits.transport.body_bytes).map(Arc::new))
                .transpose()?;
            let config = WorkerConfig {
                limits: limits.clone(),
                resource_profile: args.remove("--resource-profile").map(PathBuf::from),
                controller_url: required(&mut args, "--controller")?,
                id: required(&mut args, "--id")?,
                incarnation: required(&mut args, "--incarnation")?,
                workspace_root: PathBuf::from(required(&mut args, "--workspace-root")?),
                codex_path: PathBuf::from(required(&mut args, "--codex")?),
                validation_executable: std::env::current_exe().map_err(|e| e.to_string())?,
                coding_capacity: number(
                    &mut args,
                    "--coding-capacity",
                    limits.worker.coding_capacity,
                )?,
                native_capacity: number(
                    &mut args,
                    "--native-capacity",
                    limits.worker.native_capacity,
                )?,
                native_endpoint: args
                    .remove("--native-endpoint")
                    .unwrap_or_else(|| "https://api.openai.com/v1/responses".into()),
            };
            done(args)?;
            worker::run_with_machine_resources(config, host_profile).await
        }
        "list" => {
            let endpoint = required(&mut args, "--controller")?;
            done(args)?;
            let reply = ApiClient::new(endpoint, limits.clone())?
                .call(Operation::ListTasks {
                    after: None,
                    limit: limits.product.page_items,
                })
                .await?;
            println!(
                "{}",
                serde_json::to_string_pretty(&reply).map_err(|e| e.to_string())?
            );
            Ok(())
        }
        _ => Err("expected controller, worker or list".into()),
    }
}
