//! Optional collaboration policy; selected values are stored in limits metadata.
//! Historical absence does not acquire the current seed's workflow capabilities.
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WorkflowLimits {
    /// Maximum canonical JSON bytes of any workflow aggregate/immutable record.
    pub record_bytes: usize,
    /// Maximum UTF-8 bytes of each prompt, comment, reason, label or summary.
    pub text_bytes: usize,
    pub options: usize,
    /// Maximum total aggregate heads across workflow domains; no expiry semantics.
    pub records: usize,
    /// Maximum total immutable versions/answers/actions/comments/events across domains.
    pub revisions: usize,
    pub page_items: usize,
    pub page_bytes: usize,
    pub waits_per_task: usize,
    pub graph_scan_nodes: usize,
    pub events_per_mutation: usize,
    pub event_bytes: usize,
    pub notice_bytes: usize,
    pub pending_delivery_bytes: usize,
    pub delivery_scan_rows: usize,
}
impl WorkflowLimits {
    pub fn validate(&self, current: &super::RuntimeLimits) -> Result<(), String> {
        for (name, n) in [
            ("record_bytes",self.record_bytes),("text_bytes",self.text_bytes),
            ("options",self.options),("records",self.records),("revisions",self.revisions),
            ("page_items",self.page_items),("page_bytes",self.page_bytes),
            ("waits_per_task",self.waits_per_task),("graph_scan_nodes",self.graph_scan_nodes),
            ("events_per_mutation",self.events_per_mutation),("event_bytes",self.event_bytes),
            ("notice_bytes",self.notice_bytes),("pending_delivery_bytes",self.pending_delivery_bytes),
            ("delivery_scan_rows",self.delivery_scan_rows),
        ] {
            if n == 0 || n > isize::MAX as usize {
                return Err(format!("workflow.{name} is outside the addressable positive range"));
            }
        }
        for (name,n,maximum) in [
            ("text_bytes",self.text_bytes,self.record_bytes),
            ("record_bytes",self.record_bytes,current.transport.body_bytes),
            ("page_items",self.page_items,current.controller.collaboration_page_items),
            ("page_bytes",self.page_bytes,current.controller.collaboration_page_bytes),
            ("event_bytes",self.event_bytes,self.record_bytes),
            ("pending_delivery_bytes",self.pending_delivery_bytes,current.transport.body_bytes),
            ("notice_bytes",self.notice_bytes,self.pending_delivery_bytes),
        ] {
            if n > maximum { return Err(format!("workflow.{name} exceeds its selected enclosing bound")); }
        }
        if self.events_per_mutation > u32::MAX as usize {
            return Err("workflow.events_per_mutation exceeds event ordinal representation".into());
        }
        Ok(())
    }
}
// IDs/cursors still use transport.identity_bytes/controller.collaboration_cursor_bytes.
// Response/frame/request bounds still apply; fitting values do not prove combined fit.
// Accepted native/Codex input limits remain unchanged and are checked at delivery.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn historical_absence_roundtrips_without_enabling_workflows() {
        let mut historical = crate::limits::RuntimeLimits::seed().unwrap();
        historical.workflow = None;
        let bytes = serde_json::to_vec(&historical).unwrap();
        assert!(!serde_json::from_slice::<serde_json::Value>(&bytes).unwrap()
            .as_object().unwrap().contains_key("workflow"));
        let decoded = crate::limits::RuntimeLimits::parse(&bytes).unwrap();
        assert!(decoded.workflow.is_none());
        assert_eq!(serde_json::to_vec(&decoded).unwrap(), bytes);
        assert!(crate::limits::RuntimeLimits::seed().unwrap().workflow.is_some());
    }

    #[test]
    fn configured_fields_have_no_missing_or_duplicate_defaults() {
        let seed = crate::limits::RuntimeLimits::seed().unwrap();
        let policy = serde_json::to_value(seed.workflow.as_ref().unwrap()).unwrap();
        for name in policy.as_object().unwrap().keys() {
            let mut missing = policy.clone();
            missing.as_object_mut().unwrap().remove(name);
            assert!(serde_json::from_value::<WorkflowLimits>(missing).is_err(), "{name}");
            let mut zero = policy.clone();
            zero[name] = serde_json::json!(0);
            let zero: WorkflowLimits = serde_json::from_value(zero).unwrap();
            assert!(zero.validate(&seed).unwrap_err().contains(name), "{name}");
        }
        let raw = serde_json::to_string(&policy).unwrap();
        let duplicate = raw.replacen('{', "{\"record_bytes\":1,", 1);
        assert!(serde_json::from_str::<WorkflowLimits>(&duplicate).is_err());
        let mut unknown = policy;
        unknown["unknown_policy"] = serde_json::json!(1);
        assert!(serde_json::from_value::<WorkflowLimits>(unknown).is_err());
    }

    #[test]
    fn enclosing_bounds_refuse_without_clamping() {
        let seed = crate::limits::RuntimeLimits::seed().unwrap();
        for field in ["text_bytes", "record_bytes", "page_items", "page_bytes", "event_bytes",
            "pending_delivery_bytes", "notice_bytes"] {
            let mut policy = seed.workflow.clone().unwrap();
            match field {
                "text_bytes" => policy.text_bytes = policy.record_bytes + 1,
                "record_bytes" => policy.record_bytes = seed.transport.body_bytes + 1,
                "page_items" => policy.page_items = seed.controller.collaboration_page_items + 1,
                "page_bytes" => policy.page_bytes = seed.controller.collaboration_page_bytes + 1,
                "event_bytes" => policy.event_bytes = policy.record_bytes + 1,
                "pending_delivery_bytes" => policy.pending_delivery_bytes = seed.transport.body_bytes + 1,
                "notice_bytes" => policy.notice_bytes = policy.pending_delivery_bytes + 1,
                _ => unreachable!(),
            }
            let exact = policy.clone();
            assert!(policy.validate(&seed).unwrap_err().contains(field), "{field}");
            assert_eq!(policy, exact);
        }
    }
}
