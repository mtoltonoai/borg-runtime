//! Shared, data-only identifiers for the explicit collaboration loop receipts.
//! Names identify expected receipts; controller validation still checks their
//! exact owner, bodies, outcome, commit order and accepted profile.
pub fn reservation_id(attempt: &str, index: u32) -> String {
    format!("{attempt}-native-request-{index}")
}
pub fn usage_id(attempt: &str, index: u32) -> String {
    format!("{attempt}-native-usage-{index}")
}
pub fn call_id(attempt: &str, index: u32) -> String {
    format!("native-collab-{attempt}-{index}")
}
pub fn completion_id(attempt: &str) -> String {
    format!("{attempt}-native-loop-completed")
}
pub fn evidence(attempt: &str) -> String {
    format!("native collaboration loop: {}", completion_id(attempt))
}

/// Stable occurrence identity; the first call preserves the original contract.
pub fn call_operation_id(attempt: &str, request_index: u32, call_index: u32) -> String {
    if call_index == 1 {
        call_id(attempt, request_index)
    } else {
        format!("{}-call-{call_index}", call_id(attempt, request_index))
    }
}
