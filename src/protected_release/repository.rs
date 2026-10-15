//! Local Git fast-forward adapter only. No remote/provider or deployment calls.
use super::*;
use std::process::Stdio;
use tokio::process::Command;
async fn git(c: &Config, args: &[&str], limits: &InterfaceLimits) -> Result<String, String> {
    let output_read_bound = u64::try_from(limits.protected_git_stdout_bytes)
        .ok()
        .and_then(|n| n.checked_add(1))
        .ok_or("protected git output byte limit cannot be represented")?;
    let path = &c.repository_path;
    if !path.is_dir()
        || path.ancestors().any(|p| {
            std::fs::symlink_metadata(p)
                .map(|m| m.file_type().is_symlink())
                .unwrap_or(true)
        })
    {
        return Err("repository path missing/aliased".into());
    }
    let mut child = Command::new("/usr/bin/git")
        .arg("--no-replace-objects")
        .arg(format!("--git-dir={}", path.display()))
        .args([
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "core.fsmonitor=false",
            "-c",
            "core.logAllRefUpdates=true",
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
        .env("GIT_COMMITTER_NAME", "Borg protected release")
        .env("GIT_COMMITTER_EMAIL", "local@invalid")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| e.to_string())?;
    let stdout = child.stdout.take().ok_or("git output missing")?;
    let mut bytes = Vec::new();
    let read = tokio::time::timeout(
        Duration::from_millis(limits.protected_git_timeout_ms),
        stdout.take(output_read_bound).read_to_end(&mut bytes),
    )
    .await;
    if !matches!(read, Ok(Ok(_))) || bytes.len() > limits.protected_git_stdout_bytes {
        let _ = child.kill().await;
        let _ = child.wait().await;
        return Err("bounded git observation unavailable".into());
    }
    let status = match tokio::time::timeout(
        Duration::from_millis(limits.protected_git_timeout_ms),
        child.wait(),
    )
    .await
    {
        Ok(Ok(s)) => s,
        _ => {
            let _ = child.kill().await;
            let _ = child.wait().await;
            return Err("git completion unavailable".into());
        }
    };
    if !status.success() {
        return Err("git refused or failed".into());
    }
    String::from_utf8(bytes).map_err(|_| "git returned nontext".into())
}
fn marker(a: &Action) -> String {
    format!("borg-release:{}:{}", a.id, a.subject.expected_base)
}
fn bound(c: &Config, a: &Action) -> bool {
    a.kind == Kind::Merge
        && a.subject.repository == c.repository
        && a.subject.target_ref == c.target_ref
        && a.subject.policy == c.policy
}
//= spec/impl/v2/implementation.md#configured-actions-and-automatic-merge
//# An agent MAY merge a change after protected checks have passed for its exact revision.
// This qualified local path admits only an exact checked fast-forward and CAS.
pub async fn execute(c: &Config, a: &Action, limits: &InterfaceLimits) -> (State, String) {
    if !bound(c, a) || a.state != State::Started {
        return (State::Refused, "action binding invalid".into());
    }
    let s = &a.subject;
    if git(c, &["rev-parse", "--is-bare-repository"], limits)
        .await
        .as_ref()
        .map(|v| v.trim())
        != Ok("true")
    {
        return (
            State::Refused,
            "configured protected repository must be a bare local repository".into(),
        );
    }
    let head = match git(
        c,
        &[
            "rev-parse",
            "--verify",
            &format!("{}^{{commit}}", s.target_ref),
        ],
        limits,
    )
    .await
    {
        Ok(v) => v.trim().to_string(),
        Err(e) => return (State::Refused, e),
    };
    if head != s.expected_base {
        return (State::Refused, "expected merge base moved".into());
    }
    if git(
        c,
        &[
            "merge-base",
            "--is-ancestor",
            &s.expected_base,
            &s.artifact.source_commit,
        ],
        limits,
    )
    .await
    .is_err()
    {
        return (
            State::Refused,
            "candidate is not an exact fast-forward commit".into(),
        );
    }
    let tree = git(
        c,
        &[
            "rev-parse",
            "--verify",
            &format!("{}^{{tree}}", s.artifact.source_commit),
        ],
        limits,
    )
    .await;
    if tree.as_ref().map(|v| v.trim()) != Ok(s.artifact.source_tree.as_str()) {
        return (
            State::Refused,
            "candidate tree differs from artifact".into(),
        );
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(u64::MAX);
    if a.dispatch_until_ms.is_none_or(|until| now >= until) {
        return (
            State::Refused,
            "dispatch authorization expired before CAS".into(),
        );
    }
    // This is the action. Never replace expected_base with a refreshed observation.
    let marker = marker(a);
    let result = git(
        c,
        &[
            "update-ref",
            "--create-reflog",
            "-m",
            &marker,
            &s.target_ref,
            &s.artifact.source_commit,
            &s.expected_base,
        ],
        limits,
    )
    .await;
    match result {
        Ok(_) => observe(c, a, limits).await,
        Err(_) => {
            let (st, msg) = observe(c, a, limits).await;
            if st == State::Applied {
                (st, msg)
            } else {
                (State::Unknown,"CAS result unavailable; original intent requires readback, never automatic resubmission".into())
            }
        }
    }
}
pub async fn observe(c: &Config, a: &Action, limits: &InterfaceLimits) -> (State, String) {
    if !bound(c, a) {
        return (
            State::Unknown,
            "configured repository/policy differs from historical action".into(),
        );
    }
    let max_count = format!("--max-count={}", limits.protected_reflog_records);
    let expected = format!("{} {}", a.subject.artifact.source_commit, marker(a));
    match git(
        c,
        &[
            "reflog",
            "show",
            &max_count,
            "--format=%H %gs",
            &a.subject.target_ref,
        ],
        limits,
    )
    .await
    {
        Ok(v) if v.lines().any(|line| line == expected) => (
            State::Applied,
            "exact original CAS reflog witness observed; not current-tip or rollout authority"
                .into(),
        ),
        _ => (
            State::Unknown,
            "original CAS witness unavailable; no proof of nonexecution and no redo".into(),
        ),
    }
}
