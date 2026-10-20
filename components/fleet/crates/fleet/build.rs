//! Bakes the build revision into the binary as `FLEET_BUILD_REV` so `fleet version` can report which commit
//! the running binary was built from — the signal that was missing when a stale deployed binary silently ran
//! old logic. Precedence: the flake-supplied `FLEET_BUILD_REV` (the nix hermetic build has no `.git`), else
//! `git rev-parse` (a dev cargo build, with a `-dirty` marker for an uncommitted tree), else `unknown`. Never
//! fails the build.

use std::process::Command;

fn main() {
    // Resolve Git metadata from the crate directory: Fleet may live inside a larger repository
    // or a worktree, where ../../.git is not the repository's metadata directory.
    println!("cargo:rerun-if-env-changed=FLEET_BUILD_REV");
    for name in ["HEAD", "index", "packed-refs"] {
        watch_git_path(name);
    }
    if let Ok(out) = Command::new("git").args(["symbolic-ref", "--quiet", "HEAD"]).output() {
        if out.status.success() {
            if let Ok(name) = String::from_utf8(out.stdout) {
                watch_git_path(name.trim());
            }
        }
    }

    let rev = std::env::var("FLEET_BUILD_REV")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty() && s != "unknown")
        .or_else(git_short_rev)
        .unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=FLEET_BUILD_REV={rev}");
}

fn watch_git_path(name: &str) {
    if let Ok(out) = Command::new("git").args(["rev-parse", "--git-path", name]).output() {
        if out.status.success() {
            if let Ok(path) = String::from_utf8(out.stdout) {
                let path = path.trim();
                if !path.is_empty() {
                    println!("cargo:rerun-if-changed={path}");
                }
            }
        }
    }
}

/// The short HEAD rev of a dev checkout, with a `-dirty` suffix when the working tree has uncommitted changes.
/// `None` when git is unavailable or there is no repo (e.g. the nix sandbox) — the caller then falls back.
fn git_short_rev() -> Option<String> {
    let out = Command::new("git").args(["rev-parse", "--short", "HEAD"]).output().ok()?;
    if !out.status.success() {
        return None;
    }
    let rev = String::from_utf8(out.stdout).ok()?.trim().to_string();
    if rev.is_empty() {
        return None;
    }
    let dirty = Command::new("git")
        .args(["status", "--porcelain"])
        .output()
        .ok()
        .map(|o| !o.stdout.is_empty())
        .unwrap_or(false);
    Some(if dirty { format!("{rev}-dirty") } else { rev })
}
