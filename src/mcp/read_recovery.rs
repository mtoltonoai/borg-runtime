//! Closed retry eligibility of the canonical platform read adapters.
//! This does not establish absence of an outcome or grant caller/profile authority.
//! A validated original-call lookup must precede every redispatch, with the same
//! complete Invocation. Committed refusals and malformed receipts are not retries.
pub(crate) fn retryable_read_tool(name: &str) -> bool {
    matches!(name,
        "collab_tasks_list"
        | "collab_task_get"
        | "collab_task_history_list"
        | "collab_agents_list"
        | "collab_agent_get"
        | "collab_definition_get"
        | "collab_memory_list"
        | "collab_memory_get"
        | "collab_workers_list"
        | "collab_worker_get"
        | "collab_machines_list"
        | "collab_machine_get"
        | "collab_task_attempts_list"
        | "collab_task_checkpoints_list"
        | "collab_task_releases_list"
        | "collab_release_get"
        | "collab_release_checks_list"
        | "collab_call_get"
        | "collab_repository_tree"
        | "collab_repository_read"
        | "collab_repository_search"
        | "collab_repository_diff"
        | "collab_repository_history"
        | "collab_session_get"
        | "collab_sessions_list"
        | "collab_limits_get"
        | "collab_repositories_list"
        | "collab_repository_get"
        | "collab_question_get"
        | "collab_questions_list"
        | "collab_question_answers"
        | "collab_question_actions"
        | "collab_decisions_list"
        | "collab_document_get"
        | "collab_documents_list"
        | "collab_document_versions_list"
        | "collab_comments_list"
        | "collab_review_get"
        | "collab_reviews_list"
        | "collab_review_target_get"
        | "collab_task_coordination_get"
        | "collab_task_waits"
    )
}

#[cfg(test)]
#[path = "read_recovery_tests.rs"]
mod tests;
