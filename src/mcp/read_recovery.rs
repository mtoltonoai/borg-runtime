//! Closed retry eligibility of the canonical platform read adapters.
//! This does not establish absence of an outcome or grant caller/profile authority.
//! A validated original-call lookup must precede every redispatch, with the same
//! complete Invocation. Committed refusals and malformed receipts are not retries.
pub(crate) fn retryable_read_tool(name: &str) -> bool {
    name == "collab_read_continue" || crate::read_page::ReadTool::parse(name).is_some()
}

#[cfg(test)]
#[path = "read_recovery_tests.rs"]
mod tests;
