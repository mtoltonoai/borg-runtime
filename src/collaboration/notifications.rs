//! Bounded delivery of committed workflow events to an existing session.
//! Preparing/receiving an input is not proof the model understood that input.
use super::workflow::{DeliveryCursor, EventId, WorkflowActivity, WorkflowContinuation};
use crate::mcp::Caller;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DeliveryPage {
    pub from_cursor: DeliveryCursor,
    pub to_cursor: DeliveryCursor,
    pub through: EventId,
    pub items: Vec<WorkflowActivity>,
    /// Exact admitted wait outcomes, appended once with this same batch.
    pub continuation: Option<WorkflowContinuation>,
    pub has_more: bool,
    /// Independent bounded scan of owed immutable routing obligations.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub routing: Option<DeliveryRoutingPage>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DeliveryRoutingPage {
    pub from_cursor: Option<EventId>,
    /// None completes this scan through the outer page's `through` and wraps.
    pub to_cursor: Option<EventId>,
    pub items: Vec<WorkflowActivity>,
    pub has_more: bool,
}
impl DeliveryPage {
    pub fn has_input(&self) -> bool {
        !self.items.is_empty() || self.continuation.is_some()
            || self.routing.as_ref().is_some_and(|r| !r.items.is_empty())
    }
    /// Exact immutable occurrence carried by this input, including admitted waits.
    pub fn contains_event(&self, event: &EventId) -> bool {
        self.items.iter().any(|item| &item.id == event)
            || self.routing.as_ref().is_some_and(|r| r.items.iter().any(|item| &item.id == event))
            || self.continuation.as_ref().is_some_and(|c| c.resolved_waits.iter().any(|wait| &wait.event_id == event))
    }
    pub fn has_more_work(&self) -> bool {
        self.has_more || self.routing.as_ref().is_some_and(|r| r.has_more)
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DeliveryBatch {
    /// The original prepare operation ID, never a second allocated occurrence.
    pub batch_id: String,
    pub prepared_by: Caller,
    pub delivery: Option<DeliveryIntent>,
    pub page: DeliveryPage,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DeliveryObservation {
    pub acknowledged_cursor: DeliveryCursor,
    pub pending: Option<DeliveryBatch>,
    pub available: Option<DeliveryPage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub routing_cursor: Option<EventId>,
    /// Internal fair lane preference; not an occurrence identifier or counter.
    #[serde(default, skip_serializing_if = "is_false")]
    pub routing_turn: bool,
    /// Exact committed no-append Begin result; never an input ACK.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub superseded: Option<DeliverySupersession>,
}

fn is_false(value: &bool) -> bool { !*value }

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DeliverySupersession {
    pub batch_id: String,
    pub operation_id: String,
    pub caller: Caller,
    pub context_binding: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DeliveryIntent {
    pub operation_id: String,
    pub caller: Caller,
    pub context_binding: String,
}
