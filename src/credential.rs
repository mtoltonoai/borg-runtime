//! Finite checks for credentials in durable repository identities.
//! This does not discover unknown credentials in arbitrary paths or query data.

/// Git identities may use local paths or SSH users without embedding a secret.
/// Network URL userinfo is refused, except for a password-free SSH username.
pub(crate) fn repository_contains_credentials(value: &str) -> bool {
    // Absolute and explicitly relative paths are local Git operands. A URL-like
    // directory name does not turn a local path into a network authority.
    if value.starts_with('/') || value.starts_with("./") || value.starts_with("../") {
        return false;
    }
    let Some((prefix, rest)) = value.split_once("://") else {
        // Ordinary Git scp-style user@host:path contains no password field.
        return false;
    };
    // A scheme must occur at the start of the operand (or of an explicit
    // helper address), not inside a local path or scp-style remote path.
    if !prefix.split("::").all(|part| {
        part.as_bytes().first().is_some_and(u8::is_ascii_alphabetic)
            && part
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"+.-".contains(&byte))
    }) {
        return false;
    }
    // A remote-helper prefix (for example https::https://...) must not hide the
    // URL that Git ultimately consumes. No helper is executed by this check.
    let scheme = prefix.rsplit("::").next().unwrap_or(prefix);
    let url = &value[prefix.len() - scheme.len()..];
    if let Ok(parsed) = reqwest::Url::parse(url) {
        if parsed.password().is_some() {
            return true;
        }
        if !parsed.username().is_empty() {
            return !matches!(parsed.scheme(), "ssh" | "git+ssh" | "ssh+git");
        }
    }
    // Invalid URLs can still be echoed by Git. Refuse recognizable credential
    // authorities without serializing a parser error containing the operand.
    let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
    let Some((userinfo, _)) = authority.rsplit_once('@') else {
        return false;
    };
    if userinfo.is_empty() {
        return false;
    }
    let ssh = matches!(
        scheme.to_ascii_lowercase().as_str(),
        "ssh" | "git+ssh" | "ssh+git"
    );
    !ssh || userinfo.contains(':')
}

#[cfg(test)]
mod tests {
    use super::repository_contains_credentials as contains;

    #[test]
    fn network_secret_authorities_are_not_durable_repository_identities() {
        for value in [
            "https://user:synthetic-password@example.invalid/repo",
            "https://synthetic-token@example.invalid/repo",
            "http://user:p%40ss@example.invalid/repo",
            "ftp://user:password@example.invalid/repo",
            "ssh://git:password@example.invalid/repo",
            "git+ssh://git:password@example.invalid/repo",
            "https::https://user:password@example.invalid/repo",
            "https://user:password@[invalid/repo",
        ] {
            assert!(contains(value), "synthetic credential form was admitted");
        }
    }

    #[test]
    fn local_and_password_free_ssh_identities_remain_valid() {
        for value in [
            "/tmp/repository.git",
            "./repository.git",
            "../repository.git",
            "/tmp/https://user:password@directory/repo",
            "local/https://user:password@directory/repo",
            "git@example.invalid:repo/https://user:password@directory",
            "git@example.invalid:repo::https://user:password@directory",
            "repository.git",
            "git@example.invalid:org/repo",
            "ssh://git@example.invalid/org/repo",
            "git+ssh://git@example.invalid/org/repo",
            "ssh+git://git@example.invalid/repo",
            "https://example.invalid/repo",
            "file:///tmp/repo",
        ] {
            assert!(!contains(value), "credential-free form was refused");
        }
    }
}
