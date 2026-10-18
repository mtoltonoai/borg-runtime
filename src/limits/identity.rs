//! Closed, operator-selected mechanisms. These fields never come from a tool caller.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct IdentityAdministrationPolicy {
    pub version: u32,
    pub revision: String,
    pub enabled: bool,
    pub manager_agent_id: String,
    /// Permits the concrete complete-definition mechanism, including executor
    /// and tool selection. Mandatory validation and governance still apply.
    pub complete_definitions: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TaskCreationPolicy {
    pub version: u32,
    pub revision: String,
    pub enabled: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CollaborationCapabilities {
    pub version: u32,
    pub task_creation: bool,
    pub identity_administration: bool,
}
impl CollaborationCapabilities {
    pub fn validate(&self) -> Result<(), String> {
        if self.version != 1 { return Err("unsupported collaboration.capabilities.version".into()); }
        Ok(())
    }
}
fn identity(value: &str, limits: &super::TransportLimits) -> bool {
    !value.is_empty() && value.len() <= limits.identity_bytes && !value.chars().any(char::is_control)
}
impl IdentityAdministrationPolicy {
    pub fn validate(&self, limits: &super::TransportLimits) -> Result<(), String> {
        if self.version != 1 || !identity(&self.revision, limits) || !identity(&self.manager_agent_id, limits) {
            return Err("invalid identity_administration policy".into());
        }
        Ok(())
    }
}
impl TaskCreationPolicy {
    pub fn validate(&self, limits: &super::TransportLimits) -> Result<(), String> {
        if self.version != 1 || !identity(&self.revision, limits) { return Err("invalid task_creation policy".into()); }
        Ok(())
    }
}
