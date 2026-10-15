//! Bounded, local, read-only Git object access. The controller resolves Sources
//! from accepted task/checkpoint records; models never choose a host path/URL.
//! This reader is NOT authorization: the controller must recheck its exact
//! occurrence, caller/currentness and source tuple before committing the result.
use crate::limits::RepositoryLimits;
use serde::{Deserialize, Serialize};
use std::{
    path::{Component, Path, PathBuf},
    process::Stdio,
};
use tokio::{
    io::AsyncReadExt,
    process::Command,
    time::{Duration, Instant},
};

/// Trusted startup configuration. Empty means no repository material available.
#[derive(Clone, Debug, Default)]
pub struct Config {
    pub store_root: Option<PathBuf>,
}
/// Issued by canonical preflight, not deserialized from model arguments. A diff
/// base must independently resolve to accepted task/checkpoint source material.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Source {
    pub registration: crate::repository_catalogue::Revision,
    #[serde(skip_serializing)]
    pub store_key: String,
    pub repository: String,
    pub commit: String,
    pub base_commit: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Query {
    Tree {
        path: String,
        after: Option<String>,
        limit: usize,
    },
    ReadFile {
        path: String,
        start_line: usize,
        max_lines: usize,
    },
    Search {
        path: String,
        needle: String,
    },
    Diff {
        path: String,
    },
    History {
        limit: usize,
    },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Entry {
    pub path: String,
    pub mode: String,
    pub kind: String,
    pub object: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Commit {
    pub commit: String,
    pub parents: Vec<String>,
    pub timestamp: u64,
    pub subject: String,
}
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Output {
    pub source: Source,
    /// Source text is content, not host instructions or protected evidence.
    pub untrusted: bool,
    pub data: Data,
}
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Data {
    Tree {
        entries: Vec<Entry>,
        next_after: Option<String>,
    },
    File {
        path: String,
        object: String,
        start_line: usize,
        total_lines: usize,
        text: String,
        next_line: Option<usize>,
    },
    Search {
        text: String,
    },
    Diff {
        text: String,
    },
    History {
        commits: Vec<Commit>,
    },
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Invalid,
    InvalidLimit(&'static str),
    Unavailable,
    UnavailableLimit(&'static str),
    Bound(&'static str),
    Deadline,
    NonText,
    CleanupUnverified,
}
impl Error {
    pub fn code(self) -> &'static str {
        match self {
            Self::Invalid | Self::InvalidLimit(_) => "repository_read_invalid",
            Self::Unavailable | Self::UnavailableLimit(_) => "repository_read_unavailable",
            Self::Bound(_) => "repository_read_bound",
            Self::Deadline => "repository_read_deadline",
            Self::NonText => "repository_read_nontext",
            Self::CleanupUnverified => "repository_read_cleanup_unverified",
        }
    }
    pub fn message(self, fallback: &str) -> String {
        match self {
            Self::InvalidLimit(field) | Self::UnavailableLimit(field) | Self::Bound(field) => {
                format!("{fallback}: metadata limit {field} exceeded")
            }
            _ => fallback.into(),
        }
    }
}
type Result<T> = std::result::Result<T, Error>;
fn oid(v: &str) -> bool {
    matches!(v.len(), 40 | 64)
        && v.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn path(v: &str, empty: bool) -> bool {
    (empty || !v.is_empty())
        && !v.starts_with('/')
        && !v.ends_with('/')
        && !v.chars().any(char::is_control)
        && !v.contains('\\')
        && (v.is_empty()
            || v.split('/')
                .all(|c| !c.is_empty() && c != "." && c != ".." && !c.eq_ignore_ascii_case(".git")))
}
fn query_path(v: &str, empty: bool, limits: &RepositoryLimits) -> Result<()> {
    if !path(v, empty) {
        return Err(Error::Invalid);
    }
    if v.len() > limits.path_bytes {
        return Err(Error::InvalidLimit("repository.path_bytes"));
    }
    Ok(())
}
fn query_count(value: usize, limit: usize, field: &'static str) -> Result<()> {
    if value == 0 {
        return Err(Error::Invalid);
    }
    if value > limit {
        return Err(Error::InvalidLimit(field));
    }
    Ok(())
}
impl Query {
    pub fn validate(&self, limits: &RepositoryLimits) -> Result<()> {
        match self {
            Self::Tree {
                path: p,
                after,
                limit,
            } => {
                query_path(p, true, limits)?;
                query_count(
                    *limit,
                    limits.entries_per_page,
                    "repository.entries_per_page",
                )?;
                if let Some(after) = after {
                    query_path(after, false, limits)?;
                }
            }
            Self::ReadFile {
                path: p,
                start_line,
                max_lines,
            } => {
                query_path(p, false, limits)?;
                query_count(
                    *start_line,
                    limits.start_line_max,
                    "repository.start_line_max",
                )?;
                query_count(
                    *max_lines,
                    limits.lines_per_page,
                    "repository.lines_per_page",
                )?;
            }
            Self::Search { path: p, needle } => {
                query_path(p, true, limits)?;
                if needle.is_empty() || needle.chars().any(char::is_control) {
                    return Err(Error::Invalid);
                }
                if needle.len() > limits.needle_bytes {
                    return Err(Error::InvalidLimit("repository.needle_bytes"));
                }
            }
            Self::Diff { path: p } => query_path(p, true, limits)?,
            Self::History { limit } => {
                query_count(
                    *limit,
                    limits.entries_per_page,
                    "repository.entries_per_page",
                )?;
            }
        };
        Ok(())
    }
}
/// Read only within one configured local bare repository. This does not enforce
/// hostile same-UID filesystem isolation; configured repository maintenance is
/// trusted. Concurrent pruning/replacement may refuse a read, never fetch it.
fn repository(
    config: &Config,
    limits: &RepositoryLimits,
    source: &Source,
    deadline: Instant,
) -> Result<PathBuf> {
    if !oid(&source.commit) || source.base_commit.as_deref().is_some_and(|s| !oid(s)) {
        return Err(Error::Invalid);
    }
    if !crate::repository_config::valid_store_key(&source.store_key) {
        return Err(Error::Unavailable);
    }
    let root = config.store_root.as_ref().ok_or(Error::Unavailable)?;
    let p = root.join(&source.store_key);
    if p.as_os_str().as_encoded_bytes().len() > limits.path_bytes {
        return Err(Error::UnavailableLimit("repository.path_bytes"));
    }
    if !p.is_absolute()
        || p.components()
            .any(|c| matches!(c, Component::ParentDir | Component::CurDir))
    {
        return Err(Error::Unavailable);
    }
    for ancestor in p.ancestors() {
        let m = std::fs::symlink_metadata(ancestor).map_err(|_| Error::Unavailable)?;
        if !m.is_dir() || m.file_type().is_symlink() {
            return Err(Error::Unavailable);
        }
    }
    // Never follow alternates, worktree indirection, sockets/FIFOs or symlinked
    // Git metadata/object storage. The content tree's symlink blobs are merely
    // listed; read_file rejects them rather than interpreting link targets.
    for n in [
        "commondir",
        "gitdir",
        "objects/info/alternates",
        "objects/info/http-alternates",
    ] {
        if std::fs::symlink_metadata(p.join(n)).is_ok() {
            return Err(Error::Unavailable);
        }
    }
    let mut todo = vec![p.clone()];
    let mut count = 0usize;
    while let Some(dir) = todo.pop() {
        if Instant::now() >= deadline {
            return Err(Error::Deadline);
        }
        for entry in std::fs::read_dir(dir).map_err(|_| Error::Unavailable)? {
            if Instant::now() >= deadline {
                return Err(Error::Deadline);
            }
            count = count
                .checked_add(1)
                .ok_or(Error::Bound("repository.metadata_entries"))?;
            if count > limits.metadata_entries {
                return Err(Error::Bound("repository.metadata_entries"));
            }
            let entry = entry.map_err(|_| Error::Unavailable)?;
            let kind = entry.file_type().map_err(|_| Error::Unavailable)?;
            if kind.is_symlink() || !(kind.is_dir() || kind.is_file()) {
                return Err(Error::Unavailable);
            }
            if kind.is_dir() {
                todo.push(entry.path())
            }
        }
    }
    Ok(p.clone())
}
async fn terminate(child: &mut tokio::process::Child, limits: &RepositoryLimits) -> Result<()> {
    let _ = child.start_kill();
    match tokio::time::timeout(
        Duration::from_millis(limits.cleanup_timeout_ms),
        child.wait(),
    )
    .await
    {
        Ok(Ok(_)) => Ok(()),
        _ => Err(Error::CleanupUnverified),
    }
}
// Debug qualification can hold one real blob reader before pipe consumption.
// It cannot choose an executable, change arguments/output or extend the deadline.
#[cfg(debug_assertions)]
mod qualification {
    use super::*;
    use std::{
        collections::BTreeMap,
        sync::{Mutex, OnceLock},
    };
    use tokio::sync::oneshot;
    struct Probe {
        ready: oneshot::Sender<u32>,
        resume: oneshot::Receiver<()>,
    }
    static PROBE: OnceLock<Mutex<BTreeMap<PathBuf, Probe>>> = OnceLock::new();
    pub fn arm(repo: PathBuf) -> (oneshot::Receiver<u32>, oneshot::Sender<()>) {
        let (ready, entered) = oneshot::channel();
        let (resume, released) = oneshot::channel();
        let mut slot = PROBE
            .get_or_init(|| Mutex::new(BTreeMap::new()))
            .lock()
            .unwrap();
        assert!(!slot.contains_key(&repo), "one probe per repository");
        slot.insert(
            repo,
            Probe {
                ready,
                resume: released,
            },
        );
        (entered, resume)
    }
    pub async fn pause(
        repo: &Path,
        args: &[String],
        child: &mut tokio::process::Child,
        deadline: Instant,
        limits: &RepositoryLimits,
    ) -> Result<()> {
        let probe = {
            let mut slot = PROBE
                .get_or_init(|| Mutex::new(BTreeMap::new()))
                .lock()
                .unwrap();
            if slot.contains_key(repo)
                && args.first().map(String::as_str) == Some("cat-file")
                && args.get(1).map(String::as_str) == Some("blob")
            {
                slot.remove(repo)
            } else {
                None
            }
        };
        if let Some(probe) = probe {
            let _ = probe.ready.send(child.id().ok_or(Error::Unavailable)?);
            if !matches!(
                tokio::time::timeout_at(deadline, probe.resume).await,
                Ok(Ok(()))
            ) {
                terminate(child, limits).await?;
                return Err(Error::Deadline);
            }
        }
        Ok(())
    }
}
#[cfg(debug_assertions)]
pub fn qualification_pause_next_blob(
    repo: PathBuf,
) -> (
    tokio::sync::oneshot::Receiver<u32>,
    tokio::sync::oneshot::Sender<()>,
) {
    qualification::arm(repo)
}

async fn git(
    repo: &Path,
    limits: &RepositoryLimits,
    args: &[String],
    deadline: Instant,
    (limit, field): (usize, &'static str),
    empty_status_one: bool,
) -> Result<Vec<u8>> {
    if Instant::now() >= deadline {
        return Err(Error::Deadline);
    }
    let mut child = Command::new("/usr/bin/git")
        .args(["--no-pager", "--no-replace-objects", "--literal-pathspecs"])
        .arg(format!("--git-dir={}", repo.display()))
        .args([
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "core.fsmonitor=false",
            "-c",
            "credential.helper=",
            "-c",
            "protocol.allow=never",
            "-c",
            "diff.external=",
            "-c",
            "core.quotePath=true",
        ])
        .args(args)
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("HOME", "/nonexistent")
        .env("LC_ALL", "C")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_NO_LAZY_FETCH", "1")
        .env("GIT_ALLOW_PROTOCOL", "")
        .env("GIT_OPTIONAL_LOCKS", "0")
        .env("GIT_ATTR_NOSYSTEM", "1")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .map_err(|_| Error::Unavailable)?;
    #[cfg(debug_assertions)]
    qualification::pause(repo, args, &mut child, deadline, limits).await?;
    let Some(stdout) = child.stdout.take() else {
        terminate(&mut child, limits).await?;
        return Err(Error::Unavailable);
    };
    let mut bytes = Vec::new();
    match tokio::time::timeout_at(
        deadline,
        stdout
            .take(
                u64::try_from(limit)
                    .ok()
                    .and_then(|n| n.checked_add(1))
                    .ok_or(Error::Bound(field))?,
            )
            .read_to_end(&mut bytes),
    )
    .await
    {
        Ok(Ok(_)) if bytes.len() <= limit => {}
        result => {
            let why = if result.is_err() {
                Error::Deadline
            } else if bytes.len() > limit {
                Error::Bound(field)
            } else {
                Error::Unavailable
            };
            terminate(&mut child, limits).await?;
            return Err(why);
        }
    }
    let status = match tokio::time::timeout_at(deadline, child.wait()).await {
        Ok(Ok(s)) => s,
        _ => {
            terminate(&mut child, limits).await?;
            return Err(Error::Deadline);
        }
    };
    if !status.success() && !(empty_status_one && status.code() == Some(1) && bytes.is_empty()) {
        return Err(Error::Unavailable);
    }
    Ok(bytes)
}
fn strings(args: &[&str]) -> Vec<String> {
    args.iter().map(|s| (*s).into()).collect()
}
fn text(bytes: Vec<u8>) -> Result<String> {
    if bytes.contains(&0) {
        return Err(Error::NonText);
    }
    String::from_utf8(bytes).map_err(|_| Error::NonText)
}
async fn exact_commit(
    repo: &Path,
    limits: &RepositoryLimits,
    commit: &str,
    deadline: Instant,
) -> Result<()> {
    let value = git(
        repo,
        limits,
        &strings(&["rev-parse", "--verify", &format!("{commit}^{{commit}}")]),
        deadline,
        (limits.commit_probe_bytes, "repository.commit_probe_bytes"),
        false,
    )
    .await?;
    if value != format!("{commit}\n").as_bytes() {
        return Err(Error::Unavailable);
    }
    Ok(())
}
async fn tree(
    repo: &Path,
    limits: &RepositoryLimits,
    commit: &str,
    p: &str,
    recursive: bool,
    deadline: Instant,
) -> Result<Vec<Entry>> {
    let mut args = strings(&["ls-tree", "-z", "--full-tree"]);
    if recursive {
        args.push("-r".into())
    }
    args.extend(strings(&[commit, "--"]));
    if !p.is_empty() {
        args.push(p.into())
    }
    let bytes = git(
        repo,
        limits,
        &args,
        deadline,
        (limits.git_listing_bytes, "repository.git_listing_bytes"),
        false,
    )
    .await?;
    let mut result = vec![];
    for line in bytes.split(|b| *b == 0).filter(|b| !b.is_empty()) {
        let line = std::str::from_utf8(line).map_err(|_| Error::NonText)?;
        let (meta, name) = line.split_once('\t').ok_or(Error::Unavailable)?;
        let f: Vec<_> = meta.split(' ').collect();
        if f.len() != 3 || !oid(f[2]) || !path(name, false) {
            return Err(Error::Unavailable);
        }
        if name.len() > limits.path_bytes {
            return Err(Error::UnavailableLimit("repository.path_bytes"));
        }
        result.push(Entry {
            path: name.into(),
            mode: f[0].into(),
            kind: f[1].into(),
            object: f[2].into(),
        });
    }
    result.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(result)
}
pub async fn read(
    config: &Config,
    limits: &RepositoryLimits,
    source: &Source,
    query: &Query,
    deadline: Instant,
) -> Result<Output> {
    query.validate(limits)?;
    let local_deadline = Instant::now()
        .checked_add(Duration::from_millis(limits.read_timeout_ms))
        .ok_or(Error::Invalid)?;
    let deadline = deadline.min(local_deadline);
    if matches!(query, Query::Diff { .. }) != source.base_commit.is_some() {
        return Err(Error::Invalid);
    }
    // The local scan is deliberately finite and contains no database transaction.
    let repo = repository(config, limits, source, deadline)?;
    if git(
        &repo,
        limits,
        &strings(&["rev-parse", "--is-bare-repository"]),
        deadline,
        (limits.bare_probe_bytes, "repository.bare_probe_bytes"),
        false,
    )
    .await?
        != b"true\n"
    {
        return Err(Error::Unavailable);
    }
    exact_commit(&repo, limits, &source.commit, deadline).await?;
    let data = match query {
        Query::Tree {
            path: p,
            after,
            limit,
        } => {
            let mut all = tree(&repo, limits, &source.commit, p, true, deadline).await?;
            if let Some(after) = after {
                all.retain(|e| e.path > *after)
            }
            let more = all.len() > *limit;
            all.truncate(*limit);
            let next_after = if more {
                all.last().map(|e| e.path.clone())
            } else {
                None
            };
            Data::Tree {
                entries: all,
                next_after,
            }
        }
        Query::ReadFile {
            path: p,
            start_line,
            max_lines,
        } => {
            let all = tree(&repo, limits, &source.commit, p, false, deadline).await?;
            let e = all
                .iter()
                .find(|e| e.path == *p)
                .ok_or(Error::Unavailable)?;
            if e.kind != "blob" || !matches!(e.mode.as_str(), "100644" | "100755") {
                return Err(Error::Invalid);
            }
            let n = git(
                &repo,
                limits,
                &strings(&["cat-file", "-s", &e.object]),
                deadline,
                (limits.size_probe_bytes, "repository.size_probe_bytes"),
                false,
            )
            .await?;
            let n = std::str::from_utf8(&n)
                .map_err(|_| Error::Unavailable)?
                .trim()
                .parse::<usize>()
                .map_err(|_| Error::Unavailable)?;
            if n > limits.file_bytes {
                return Err(Error::Bound("repository.file_bytes"));
            }
            let content = text(
                git(
                    &repo,
                    limits,
                    &strings(&["cat-file", "blob", &e.object]),
                    deadline,
                    (limits.file_bytes, "repository.file_bytes"),
                    false,
                )
                .await?,
            )?;
            let lines: Vec<_> = content.split_inclusive('\n').collect();
            let from = start_line - 1;
            if from > lines.len() {
                return Err(Error::Invalid);
            }
            let end = from.saturating_add(*max_lines).min(lines.len());
            Data::File {
                path: p.clone(),
                object: e.object.clone(),
                start_line: *start_line,
                total_lines: lines.len(),
                text: lines[from..end].concat(),
                next_line: (end < lines.len()).then_some(end + 1),
            }
        }
        Query::Search { path: p, needle } => {
            let mut args = strings(&[
                "grep",
                "-F",
                "-n",
                "-I",
                "--no-textconv",
                "--full-name",
                "-e",
                needle,
                &source.commit,
                "--",
            ]);
            if !p.is_empty() {
                args.push(p.clone())
            }
            Data::Search {
                text: text(
                    git(
                        &repo,
                        limits,
                        &args,
                        deadline,
                        (limits.output_bytes, "repository.output_bytes"),
                        true,
                    )
                    .await?,
                )?,
            }
        }
        Query::Diff { path: p } => {
            let base = source.base_commit.as_ref().ok_or(Error::Invalid)?;
            exact_commit(&repo, limits, base, deadline).await?;
            let mut args = strings(&[
                "diff",
                "--no-ext-diff",
                "--no-textconv",
                "--no-color",
                "--no-renames",
                "--numstat",
                "-z",
                base,
                &source.commit,
                "--",
            ]);
            if !p.is_empty() {
                args.push(p.clone())
            }
            let stats = git(
                &repo,
                limits,
                &args,
                deadline,
                (limits.git_listing_bytes, "repository.git_listing_bytes"),
                false,
            )
            .await?;
            if stats.split(|b| *b == 0).any(|s| s.starts_with(b"-\t-\t")) {
                return Err(Error::NonText);
            }
            args.retain(|v| v != "--numstat" && v != "-z");
            Data::Diff {
                text: text(
                    git(
                        &repo,
                        limits,
                        &args,
                        deadline,
                        (limits.output_bytes, "repository.output_bytes"),
                        false,
                    )
                    .await?,
                )?,
            }
        }
        Query::History { limit } => {
            let bytes = git(
                &repo,
                limits,
                &strings(&[
                    "log",
                    "--no-decorate",
                    "--no-notes",
                    "--no-show-signature",
                    "--format=%H%x00%P%x00%ct%x00%s%x00",
                    &format!("--max-count={limit}"),
                    &source.commit,
                    "--",
                ]),
                deadline,
                (limits.output_bytes, "repository.output_bytes"),
                false,
            )
            .await?;
            let mut fields = bytes.split(|b| *b == 0);
            let mut commits = vec![];
            loop {
                let Some(id) = fields.next() else { break };
                let id = std::str::from_utf8(id)
                    .map_err(|_| Error::NonText)?
                    .trim_start_matches('\n');
                if id.is_empty() {
                    break;
                }
                if !oid(id) {
                    return Err(Error::Unavailable);
                }
                let mut take = || -> Result<&str> {
                    std::str::from_utf8(fields.next().ok_or(Error::Unavailable)?)
                        .map_err(|_| Error::NonText)
                };
                let parents: Vec<String> = take()?.split_whitespace().map(Into::into).collect();
                if parents.iter().any(|p| !oid(p)) {
                    return Err(Error::Unavailable);
                }
                let timestamp = take()?.parse().map_err(|_| Error::Unavailable)?;
                commits.push(Commit {
                    commit: id.into(),
                    parents,
                    timestamp,
                    subject: take()?.into(),
                });
            }
            Data::History { commits }
        }
    };
    // Traceability scope: Every successful repository operation returns the exact validated source tuple used for its read.
    //= spec/impl/v2/implementation.md#repository-reads-through-the-shared-tool-interface
    //# A repository read result MUST identify its exact source revision.
    let output = Output {
        source: source.clone(),
        untrusted: true,
        data,
    };
    if serde_json::to_vec(&output)
        .map_err(|_| Error::Bound("repository.output_bytes"))?
        .len()
        > limits.output_bytes
    {
        return Err(Error::Bound("repository.output_bytes"));
    }
    Ok(output)
}

#[cfg(test)]
mod metadata_tests {
    use super::*;
    #[test]
    fn query_limit_refusals_identify_metadata_without_relabeling_invalid_sources() {
        let mut limits = crate::limits::RuntimeLimits::seed().unwrap().repository;
        limits.path_bytes = 4;
        limits.needle_bytes = 3;
        limits.entries_per_page = 2;
        limits.start_line_max = 4;
        limits.lines_per_page = 2;
        let cases = [
            (
                Query::Tree {
                    path: "longer".into(),
                    after: None,
                    limit: 1,
                },
                "repository.path_bytes",
            ),
            (
                Query::Tree {
                    path: "".into(),
                    after: Some("longer".into()),
                    limit: 1,
                },
                "repository.path_bytes",
            ),
            (
                Query::Tree {
                    path: "".into(),
                    after: None,
                    limit: 3,
                },
                "repository.entries_per_page",
            ),
            (
                Query::ReadFile {
                    path: "longer".into(),
                    start_line: 1,
                    max_lines: 1,
                },
                "repository.path_bytes",
            ),
            (
                Query::ReadFile {
                    path: "file".into(),
                    start_line: 5,
                    max_lines: 1,
                },
                "repository.start_line_max",
            ),
            (
                Query::ReadFile {
                    path: "file".into(),
                    start_line: 1,
                    max_lines: 3,
                },
                "repository.lines_per_page",
            ),
            (
                Query::Search {
                    path: "longer".into(),
                    needle: "a".into(),
                },
                "repository.path_bytes",
            ),
            (
                Query::Search {
                    path: "".into(),
                    needle: "éé".into(),
                },
                "repository.needle_bytes",
            ),
            (
                Query::Diff {
                    path: "longer".into(),
                },
                "repository.path_bytes",
            ),
            (Query::History { limit: 3 }, "repository.entries_per_page"),
        ];
        for (query, field) in cases {
            let error = query.validate(&limits).unwrap_err();
            assert_eq!(error, Error::InvalidLimit(field));
            assert_eq!(error.code(), "repository_read_invalid");
            assert_eq!(
                error.message("repository query refused"),
                format!("repository query refused: metadata limit {field} exceeded")
            );
        }
        for query in [
            Query::Tree {
                path: "../outside".into(),
                after: None,
                limit: 1,
            },
            Query::Tree {
                path: "".into(),
                after: Some(".git/config".into()),
                limit: 1,
            },
            Query::History { limit: 0 },
            Query::ReadFile {
                path: "file".into(),
                start_line: 0,
                max_lines: 1,
            },
            Query::ReadFile {
                path: "file".into(),
                start_line: 1,
                max_lines: 0,
            },
            Query::Search {
                path: "".into(),
                needle: "".into(),
            },
            Query::Search {
                path: "".into(),
                needle: "a\nb".into(),
            },
        ] {
            assert_eq!(query.validate(&limits), Err(Error::Invalid));
        }
        assert_eq!(
            Error::Invalid.message("repository query refused"),
            "repository query refused"
        );
    }
    #[test]
    fn query_policy_moves_with_metadata_but_source_boundaries_do_not() {
        let mut limits = crate::limits::RuntimeLimits::seed().unwrap().repository;
        limits.entries_per_page = 2;
        limits.path_bytes = 4;
        limits.needle_bytes = 3;
        limits.lines_per_page = 2;
        limits.start_line_max = 4;
        assert!(Query::History { limit: 2 }.validate(&limits).is_ok());
        assert!(Query::History { limit: 3 }.validate(&limits).is_err());
        assert!(
            Query::ReadFile {
                path: "file".into(),
                start_line: 4,
                max_lines: 2
            }
            .validate(&limits)
            .is_ok()
        );
        assert!(
            Query::ReadFile {
                path: "file".into(),
                start_line: 5,
                max_lines: 2
            }
            .validate(&limits)
            .is_err()
        );
        assert!(
            Query::Search {
                path: "".into(),
                needle: "abc".into()
            }
            .validate(&limits)
            .is_ok()
        );
        assert!(
            Query::Search {
                path: "".into(),
                needle: "abcd".into()
            }
            .validate(&limits)
            .is_err()
        );
        limits.path_bytes = 1024;
        for invalid in ["../outside", "/absolute", ".git/config", "a/../b"] {
            assert!(
                Query::Tree {
                    path: invalid.into(),
                    after: None,
                    limit: 1
                }
                .validate(&limits)
                .is_err()
            );
        }
        assert!(oid(&"a".repeat(40)));
        assert!(!oid("HEAD"));
    }
}
