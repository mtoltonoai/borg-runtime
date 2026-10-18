//! Durable registration metadata; public views never contain controller storage keys.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Revision {
    pub repository_id: String,
    pub registration_revision: String,
}

/// Operator input. Registration records locations; it never provisions them.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Registration {
    /// Optional operator-owned provider target; immutable with this revision.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pull_request_target: Option<crate::pull_request::Target>,
    pub repository_id: String,
    pub registration_revision: String,
    pub display_name: String,
    pub source_endpoint: String,
    pub checkpoint_endpoint: String,
    pub source_store_key: String,
    pub checkpoint_store_key: String,
}
impl Registration {
    pub fn binding(&self) -> Revision {
        Revision {
            repository_id: self.repository_id.clone(),
            registration_revision: self.registration_revision.clone(),
        }
    }
}

/// Safe catalogue projection shared by operator UI and collaboration tools.
/// Generation is the CAS token for the current head, including enable toggles.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
//= spec/impl/v2/implementation.md#repository-associations-are-versioned-metadata
//# A model-visible repository projection MUST NOT expose controller storage keys.
pub struct Entry {
    /// Capability only; private provider routing and credential selector stay hidden.
    #[serde(default, skip_serializing_if = "is_false")]
    pub pull_request_available: bool,
    pub repository_id: String,
    pub registration_revision: String,
    pub display_name: String,
    pub source_endpoint: String,
    pub checkpoint_endpoint: String,
    pub current_revision: String,
    pub generation: u64,
    pub enabled_for_new_tasks: bool,
}

fn is_false(value: &bool) -> bool { !*value }
