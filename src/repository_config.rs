//! Trusted operational root only. Durable registration metadata selects a child
//! store key; this configuration never imports registrations or acquires Git data.
use crate::{limits::RepositoryLimits, repository_read::Config};
use std::path::{Component, Path};

pub fn valid_store_key(value: &str) -> bool {
    !value.is_empty()
        && value != "."
        && value != ".."
        && value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_' | b'.'))
}
pub fn root(path: &Path, limits: &RepositoryLimits) -> Result<Config, String> {
    if path.as_os_str().as_encoded_bytes().len() > limits.path_bytes {
        return Err("repository store root exceeds metadata limit: repository.path_bytes".into());
    }
    if !path.is_absolute()
        || path
            .components()
            .any(|c| matches!(c, Component::ParentDir | Component::CurDir))
    {
        return Err("repository store root must be an absolute plain path".into());
    }
    Ok(Config {
        store_root: Some(path.into()),
    })
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn store_keys_cannot_select_another_path() {
        for value in [
            "", ".", "..", "../a", "/a", "a/b", "a\\b", "a:b", "a\n", "a%2fb",
        ] {
            assert!(!valid_store_key(value), "{value:?}");
        }
        assert!(valid_store_key("source-1.git"));
        assert!(valid_store_key("checkpoint_2.git"));
    }
    #[test]
    fn root_is_explicit_and_bounded_without_acquisition() {
        let mut limits = crate::limits::RuntimeLimits::seed().unwrap().repository;
        let path = Path::new("/nonexistent-borg-catalogue-root");
        assert_eq!(
            root(path, &limits).unwrap().store_root.as_deref(),
            Some(path)
        );
        assert!(root(Path::new("relative"), &limits).is_err());
        assert!(root(Path::new("/tmp/../elsewhere"), &limits).is_err());
        limits.path_bytes = 2;
        assert!(
            root(path, &limits)
                .unwrap_err()
                .contains("repository.path_bytes")
        );
    }
}
