//! Pure transitions for controller-owned native circuits.
//! Call only inside the existing receipt transaction after canonical caller/policy
//! validation. Persist the returned state and original operation outcome together.
//! Completion permits must come from the original committed acquire receipt.
use crate::mcp::Caller;
use crate::native_retry::*;

/// Apply only when inserting a previously absent key; never silently evict state.
pub(super) fn validate_new_entry_count(retained: usize, max_entries: usize) -> Result<(), String> {
    if max_entries == 0 || max_entries > isize::MAX as usize {
        return Err("native circuit max_entries exceeds positive addressable representation".into());
    }
    if retained.checked_add(1).is_none_or(|count| count > max_entries) {
        return Err("native circuit retained keys exceed selected max_entries".into());
    }
    Ok(())
}

pub(super) fn validate_state(state: &CircuitState) -> Result<(), String> {
    state.key.validate()?;
    match &state.phase {
        CircuitPhase::Closed => {}
        CircuitPhase::Open { until_ms } => {
            if *until_ms == 0 || state.consecutive_failures == 0 {
                return Err("native circuit open state is invalid".into());
            }
        }
        CircuitPhase::HalfOpen { probe } => {
            validate_permit(probe)?;
            if !probe.probe || probe.key != state.key || probe.generation != state.generation
                || state.consecutive_failures == 0
            {
                return Err("native circuit probe does not match its state".into());
            }
        }
    }
    Ok(())
}

fn validate_permit(permit: &CircuitPermit) -> Result<(), String> {
    permit.key.validate()?;
    permit.policy.validate()?;
    if permit.operation_id.is_empty()
        || permit.caller.task_id.is_empty()
        || permit.caller.attempt_id.is_empty()
        || permit.caller.worker_id.is_empty()
        || permit.caller.incarnation.is_empty()
    {
        return Err("native circuit permit identity is invalid".into());
    }
    Ok(())
}

pub(super) fn acquire(
    state: Option<&CircuitState>,
    key: &CircuitKey,
    caller: &Caller,
    operation_id: &str,
    policy: &CircuitPolicy,
    now_ms: u64,
) -> Result<CircuitAcquisition, String> {
    key.validate()?;
    policy.validate()?;
    now_ms.checked_add(policy.open_ms).ok_or("native circuit open duration exceeds clock representation")?;
    let mut next = match state {
        Some(value) => {
            validate_state(value)?;
            if value.key != *key {
                return Err("native circuit state belongs to a different key".into());
            }
            value.clone()
        }
        None => CircuitState { key: key.clone(), generation: 0, consecutive_failures: 0, phase: CircuitPhase::Closed },
    };
    let mut permit = CircuitPermit {
        operation_id: operation_id.into(), caller: caller.clone(), key: key.clone(),
        generation: next.generation, probe: false, policy: policy.clone(),
    };
    validate_permit(&permit)?;
    let admission = match &next.phase {
        CircuitPhase::Closed => CircuitAdmission::Admitted { permit },
        CircuitPhase::Open { until_ms } if now_ms < *until_ms => CircuitAdmission::Open { retry_at_ms: *until_ms },
        CircuitPhase::Open { .. } => {
            permit.probe = true;
            next.phase = CircuitPhase::HalfOpen { probe: permit.clone() };
            CircuitAdmission::Admitted { permit }
        }
        CircuitPhase::HalfOpen { probe } => CircuitAdmission::ProbePending { operation_id: probe.operation_id.clone() },
    };
    Ok(CircuitAcquisition { state: next, admission })
}

//= spec/impl/v2/implementation.md#platform-owned-model-retries
//# A circuit-breaker probe outcome MUST be settled against its exact original request identity and probe generation.
pub(super) fn complete(
    state: &CircuitState,
    permit: &CircuitPermit,
    outcome: CircuitOutcome,
    now_ms: u64,
) -> Result<CircuitCompletion, String> {
    validate_state(state)?;
    validate_permit(permit)?;
    if permit.key != state.key || permit.generation > state.generation {
        return Err("native circuit completion does not match its state".into());
    }
    if permit.generation < state.generation {
        return Ok(CircuitCompletion { state: state.clone(), disposition: CircuitCompletionDisposition::StaleGeneration });
    }
    match &state.phase {
        CircuitPhase::Closed if !permit.probe => {}
        CircuitPhase::HalfOpen { probe } if permit.probe && probe == permit => {}
        _ => return Err("native circuit completion does not own the pending phase".into()),
    }
    let mut next = state.clone();
    let disposition = match outcome {
        CircuitOutcome::Healthy => {
            if permit.probe {
                next.generation = next.generation.checked_add(1).ok_or("native circuit generation exhausted")?;
            }
            next.consecutive_failures = 0;
            next.phase = CircuitPhase::Closed;
            CircuitCompletionDisposition::Applied
        }
        CircuitOutcome::TransientFailure | CircuitOutcome::NotStarted | CircuitOutcome::Unknown => {
            let is_unknown = matches!(outcome, CircuitOutcome::Unknown);
            let is_failure = !matches!(outcome, CircuitOutcome::NotStarted);
            if is_failure {
                next.consecutive_failures = next.consecutive_failures.saturating_add(1);
            }
            // A probe whose effect is unknown enters a health cooldown; its
            // original outcome stays unknown in the immutable operation receipt.
            // The next generation admits a new independent request, not a resend.
            if permit.probe || (is_failure && next.consecutive_failures >= permit.policy.failure_threshold) {
                let until_ms = now_ms.checked_add(permit.policy.open_ms)
                    .ok_or("native circuit open duration exceeds clock representation")?;
                next.generation = next.generation.checked_add(1).ok_or("native circuit generation exhausted")?;
                next.phase = CircuitPhase::Open { until_ms };
            }
            if is_unknown {
                CircuitCompletionDisposition::OutcomeUnknown {
                    retry_at_ms: match &next.phase { CircuitPhase::Open { until_ms } => Some(*until_ms), _ => None },
                }
            } else {
                CircuitCompletionDisposition::Applied
            }
        }
    };
    Ok(CircuitCompletion { state: next, disposition })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> CircuitKey {
        CircuitKey::new(&reqwest::Url::parse("https://example.invalid/v1/responses").unwrap(), "model-a").unwrap()
    }
    fn caller(worker: &str) -> Caller {
        Caller { task_id: "task".into(), attempt_id: "attempt".into(), generation: 7, worker_id: worker.into(), incarnation: "incarnation".into() }
    }
    fn policy() -> CircuitPolicy { CircuitPolicy { failure_threshold: 2, open_ms: 50 } }
    fn admitted(value: &CircuitAcquisition) -> CircuitPermit {
        match &value.admission { CircuitAdmission::Admitted { permit } => permit.clone(), other => panic!("expected permit, got {other:?}") }
    }
    fn opened() -> CircuitState {
        let one = acquire(None, &key(), &caller("worker-a"), "first", &policy(), 100).unwrap();
        let one = complete(&one.state, &admitted(&one), CircuitOutcome::TransientFailure, 101).unwrap();
        let two = acquire(Some(&one.state), &key(), &caller("worker-b"), "second", &policy(), 102).unwrap();
        complete(&two.state, &admitted(&two), CircuitOutcome::TransientFailure, 103).unwrap().state
    }

    #[test]
    fn failures_share_exact_key_and_only_one_worker_gets_the_probe() {
        let open = opened();
        let refused = acquire(Some(&open), &key(), &caller("worker-c"), "early", &policy(), 152).unwrap();
        assert_eq!(refused.admission, CircuitAdmission::Open { retry_at_ms: 153 });
        let probe = acquire(Some(&open), &key(), &caller("worker-c"), "probe", &policy(), 153).unwrap();
        assert!(admitted(&probe).probe);
        let other = acquire(Some(&probe.state), &key(), &caller("worker-d"), "competing-probe", &policy(), 154).unwrap();
        assert_eq!(other.admission, CircuitAdmission::ProbePending { operation_id: "probe".into() });
        assert_eq!(other.state, probe.state);
        let closed = complete(&probe.state, &admitted(&probe), CircuitOutcome::Healthy, 155).unwrap().state;
        assert_eq!(closed.phase, CircuitPhase::Closed);
        assert_eq!(closed.consecutive_failures, 0);
        assert!(admitted(&acquire(Some(&closed), &key(), &caller("worker-d"), "after", &policy(), 156).unwrap()).probe == false);
    }

    #[test]
    fn unknown_probe_cools_down_then_admits_only_a_new_generation() {
        let probe = acquire(Some(&opened()), &key(), &caller("worker-a"), "probe", &policy(), 153).unwrap();
        let uncertain = complete(&probe.state, &admitted(&probe), CircuitOutcome::Unknown, 154).unwrap();
        assert_eq!(uncertain.disposition, CircuitCompletionDisposition::OutcomeUnknown { retry_at_ms: Some(204) });
        let reopened: CircuitState = serde_json::from_slice(&serde_json::to_vec(&uncertain.state).unwrap()).unwrap();
        assert_eq!(acquire(Some(&reopened), &key(), &caller("worker-b"), "early", &policy(), 203).unwrap().admission,
            CircuitAdmission::Open { retry_at_ms: 204 });
        let independent = acquire(Some(&reopened), &key(), &caller("worker-b"), "independent", &policy(), 204).unwrap();
        let next = admitted(&independent);
        assert!(next.probe);
        assert_ne!(next.operation_id, admitted(&probe).operation_id);
        assert_ne!(next.generation, admitted(&probe).generation);
        let late_original = complete(&independent.state, &admitted(&probe), CircuitOutcome::Healthy, 205).unwrap();
        assert_eq!(late_original.disposition, CircuitCompletionDisposition::StaleGeneration);
        assert_eq!(late_original.state, independent.state);
    }

    #[test]
    fn elapsed_time_alone_cannot_release_an_unreported_in_flight_probe() {
        let probe = acquire(Some(&opened()), &key(), &caller("worker-a"), "probe", &policy(), 153).unwrap();
        let reopened: CircuitState = serde_json::from_slice(&serde_json::to_vec(&probe.state).unwrap()).unwrap();
        let later = acquire(Some(&reopened), &key(), &caller("worker-b"), "later", &policy(), 100_000).unwrap();
        assert_eq!(later.admission, CircuitAdmission::ProbePending { operation_id: "probe".into() });
        assert_eq!(later.state, reopened);
    }

    #[test]
    fn unknown_nonprobe_counts_health_failure_without_inventing_a_known_effect() {
        let first = acquire(None, &key(), &caller("worker-a"), "first", &policy(), 100).unwrap();
        let first = complete(&first.state, &admitted(&first), CircuitOutcome::Unknown, 101).unwrap();
        assert_eq!(first.state.phase, CircuitPhase::Closed);
        assert_eq!(first.state.consecutive_failures, 1);
        assert_eq!(first.disposition, CircuitCompletionDisposition::OutcomeUnknown { retry_at_ms: None });
        let second = acquire(Some(&first.state), &key(), &caller("worker-b"), "second", &policy(), 102).unwrap();
        let second = complete(&second.state, &admitted(&second), CircuitOutcome::Unknown, 103).unwrap();
        assert_eq!(second.state.phase, CircuitPhase::Open { until_ms: 153 });
        assert_eq!(second.disposition, CircuitCompletionDisposition::OutcomeUnknown { retry_at_ms: Some(153) });
    }

    #[test]
    //= spec/impl/v2/implementation.md#platform-owned-model-retries
    //= type=test
    //# A circuit-breaker probe outcome MUST be settled against its exact original request identity and probe generation.
    fn confirmed_unstarted_probe_reopens_but_foreign_caller_cannot_release_it() {
        let probe = acquire(Some(&opened()), &key(), &caller("worker-a"), "probe", &policy(), 153).unwrap();
        let original = admitted(&probe);
        let mut foreign = original.clone();
        foreign.caller.incarnation = "replacement-incarnation".into();
        assert!(complete(&probe.state, &foreign, CircuitOutcome::NotStarted, 154).is_err());
        let released = complete(&probe.state, &original, CircuitOutcome::NotStarted, 154).unwrap();
        assert_eq!(released.state.phase, CircuitPhase::Open { until_ms: 204 });
        assert_eq!(released.state.consecutive_failures, probe.state.consecutive_failures);
    }

    #[test]
    //= spec/impl/v2/implementation.md#platform-owned-model-retries
    //= type=test
    //# A circuit-breaker probe outcome MUST be settled against its exact original request identity and probe generation.
    fn old_nonprobe_success_or_failure_cannot_change_a_new_generation() {
        let one = acquire(None, &key(), &caller("worker-a"), "old-in-flight", &policy(), 100).unwrap();
        let old = admitted(&one);
        let open = opened();
        for outcome in [CircuitOutcome::Healthy, CircuitOutcome::TransientFailure, CircuitOutcome::Unknown] {
            let result = complete(&open, &old, outcome, 154).unwrap();
            assert_eq!(result.disposition, CircuitCompletionDisposition::StaleGeneration);
            assert_eq!(result.state, open);
        }
    }

    #[test]
    //= spec/impl/v2/implementation.md#platform-owned-model-retries
    //= type=test
    //# Retry timing, jitter and circuit-breaker bounds MUST come from versioned metadata.
    fn acquired_policy_is_retained_and_probe_failure_uses_that_policy() {
        let selected = CircuitPolicy { failure_threshold: 9, open_ms: 80 };
        let probe = acquire(Some(&opened()), &key(), &caller("worker-a"), "probe", &selected, 153).unwrap();
        let original = admitted(&probe);
        assert_eq!(original.policy, selected);
        let result = complete(&probe.state, &original, CircuitOutcome::TransientFailure, 154).unwrap();
        assert_eq!(result.state.phase, CircuitPhase::Open { until_ms: 234 });
    }

    #[test]
    fn capacity_is_explicit_and_never_evicts_an_existing_open_or_unknown_key() {
        validate_new_entry_count(1, 2).unwrap();
        for (retained, maximum) in [(2, 2), (0, 0), (usize::MAX, 2), (0, usize::MAX)] {
            assert!(validate_new_entry_count(retained, maximum).is_err());
        }
        let open = opened();
        let mut another = key();
        another.model = "model-b".into();
        assert!(acquire(Some(&open), &another, &caller("worker-a"), "wrong-key", &policy(), 154).is_err());
        assert_eq!(open.phase, CircuitPhase::Open { until_ms: 153 });
    }

    #[test]
    fn invalid_policy_clock_or_generation_refuses_without_partial_state() {
        for invalid in [CircuitPolicy { failure_threshold: 0, open_ms: 1 }, CircuitPolicy { failure_threshold: 1, open_ms: 0 }] {
            assert!(acquire(None, &key(), &caller("worker-a"), "invalid", &invalid, 100).is_err());
        }
        assert!(acquire(None, &key(), &caller("worker-a"), "overflow", &policy(), u64::MAX).is_err());
        let mut open = opened();
        open.generation = u64::MAX;
        let probe = acquire(Some(&open), &key(), &caller("worker-a"), "last-generation", &policy(), 153).unwrap();
        assert!(complete(&probe.state, &admitted(&probe), CircuitOutcome::Healthy, 154).is_err());
        assert!(matches!(probe.state.phase, CircuitPhase::HalfOpen { .. }));
    }
}
