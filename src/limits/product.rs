//! Explicit product and repository policy. Values are loaded from runtime metadata.
//! No default implementation supplies policy when required metadata is absent.
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProductLimits {
    pub request_path_bytes: usize,
    pub form_fields: usize,
    pub form_key_bytes: usize,
    pub response_bytes: usize,
    pub page_items: usize,
    pub live: LiveLimits,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiveLimits {
    pub request_timeout_ms: u64,
    pub catchup_pages_per_batch: usize,
    pub retained_activity_items: usize,
    pub max_active_fetches: usize,
    pub retry_initial_ms: u64,
    pub retry_multiplier: u32,
    pub retry_max_ms: u64,
    #[serde(deserialize_with = "required_retry_limit")]
    pub retry_limit: Option<u64>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RepositoryLimits {
    pub catalogue_bytes: u64,
    pub catalogue_entries: usize,
    pub catalogue_revisions: usize,
    pub endpoint_bytes: usize,
    pub path_bytes: usize,
    pub needle_bytes: usize,
    pub entries_per_page: usize,
    pub start_line_max: usize,
    pub lines_per_page: usize,
    pub file_bytes: usize,
    pub metadata_entries: usize,
    pub git_listing_bytes: usize,
    pub output_bytes: usize,
    pub commit_probe_bytes: usize,
    pub bare_probe_bytes: usize,
    pub size_probe_bytes: usize,
    pub read_timeout_ms: u64,
    pub cleanup_timeout_ms: u64,
    pub default_entries: usize,
    pub default_start_line: usize,
    pub default_lines: usize,
}
fn required_retry_limit<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<u64>, D::Error> {
    Option::<u64>::deserialize(d)
}
impl ProductLimits {
    pub fn validate(&self) -> Result<(), String> {
        if [
            self.request_path_bytes,
            self.form_fields,
            self.form_key_bytes,
            self.response_bytes,
            self.page_items,
            self.live.catchup_pages_per_batch,
            self.live.retained_activity_items,
            self.live.max_active_fetches,
        ]
        .contains(&0)
            || self.live.request_timeout_ms == 0
            || self.live.retry_initial_ms == 0
            || self.live.retry_multiplier == 0
            || self.live.retry_initial_ms > self.live.retry_max_ms
        {
            return Err("invalid product limits metadata".into());
        }
        let exact_integer_max = (1u64 << 53) - 1;
        if [
            self.response_bytes,
            self.page_items,
            self.live.catchup_pages_per_batch,
            self.live.retained_activity_items,
            self.live.max_active_fetches,
        ]
        .into_iter()
        .any(|n| n as u64 > exact_integer_max)
            || self.live.retry_limit.is_some_and(|n| n > exact_integer_max)
        {
            return Err("product limit exceeds exact browser integer representation".into());
        }
        // The browser timer and exact integer representations are compatibility bounds,
        // not implicit operational defaults.
        if self.live.request_timeout_ms > i32::MAX as u64
            || self.live.retry_max_ms > i32::MAX as u64
        {
            return Err("product timer exceeds browser representation".into());
        }
        Ok(())
    }
}
impl RepositoryLimits {
    pub fn validate(&self) -> Result<(), String> {
        if [
            self.catalogue_entries,
            self.catalogue_revisions,
            self.endpoint_bytes,
            self.path_bytes,
            self.needle_bytes,
            self.entries_per_page,
            self.start_line_max,
            self.lines_per_page,
            self.file_bytes,
            self.metadata_entries,
            self.git_listing_bytes,
            self.output_bytes,
            self.commit_probe_bytes,
            self.bare_probe_bytes,
            self.size_probe_bytes,
            self.default_entries,
            self.default_start_line,
            self.default_lines,
        ]
        .contains(&0)
            || self.catalogue_bytes == 0
            || self.catalogue_bytes == u64::MAX
            || self.read_timeout_ms == 0
            || self.cleanup_timeout_ms == 0
            || self.default_entries > self.entries_per_page
            || self.default_lines > self.lines_per_page
            || self.default_start_line > self.start_line_max
        {
            return Err("invalid repository limits metadata".into());
        }
        if self.catalogue_bytes > i64::MAX as u64
            || [self.catalogue_entries, self.catalogue_revisions]
                .iter()
                .any(|v| u64::try_from(*v).map_or(true, |n| n > i64::MAX as u64))
        {
            return Err("repository catalogue bound exceeds SQLite representation".into());
        }
        for ms in [self.read_timeout_ms, self.cleanup_timeout_ms] {
            if std::time::Instant::now()
                .checked_add(std::time::Duration::from_millis(ms))
                .is_none()
            {
                return Err("repository timeout exceeds clock representation".into());
            }
        }
        for value in [
            self.file_bytes,
            self.git_listing_bytes,
            self.output_bytes,
            self.commit_probe_bytes,
            self.bare_probe_bytes,
            self.size_probe_bytes,
        ] {
            if u64::try_from(value)
                .ok()
                .and_then(|v| v.checked_add(1))
                .is_none()
            {
                return Err("repository read bound exceeds stream representation".into());
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn required_metadata_has_no_numeric_fallback() {
        let limits = crate::limits::RuntimeLimits::seed().unwrap();
        let mut value = serde_json::to_value(&limits.product).unwrap();
        value.as_object_mut().unwrap().remove("response_bytes");
        assert!(serde_json::from_value::<ProductLimits>(value).is_err());
        let mut value = serde_json::to_value(&limits.product).unwrap();
        value["live"].as_object_mut().unwrap().remove("retry_limit");
        assert!(serde_json::from_value::<ProductLimits>(value).is_err());
        let mut value = serde_json::to_value(&limits.repository).unwrap();
        value["undocumented_fallback"] = serde_json::json!(1);
        assert!(serde_json::from_value::<RepositoryLimits>(value).is_err());
    }
    #[test]
    fn changed_policy_values_validate_without_recompiling_defaults() {
        let mut limits = crate::limits::RuntimeLimits::seed().unwrap();
        limits.product.live.max_active_fetches = 3;
        limits.product.live.retry_limit = Some(0);
        limits.product.live.retained_activity_items = 7;
        assert!(limits.product.validate().is_ok());
        limits.repository.entries_per_page = 3;
        limits.repository.default_entries = 3;
        assert!(limits.repository.validate().is_ok());
        limits.repository.default_entries = 4;
        assert!(limits.repository.validate().is_err());
        limits.product.live.retry_max_ms = limits.product.live.retry_initial_ms - 1;
        assert!(limits.product.validate().is_err());
    }
}
