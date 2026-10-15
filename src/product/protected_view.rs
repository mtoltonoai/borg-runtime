//! Read-only rendering of the controller's protected-release projection.
//! Record presence never substitutes for current backend authority checks.
use super::{escape, term};
use crate::protected_release::{Command, Kind, StageView, State, Subject};
use std::fmt::Write;

fn subject(body: &mut String, value: &Subject) {
    term(body, "Release candidate", &value.release_id);
    term(body, "Source commit", &value.artifact.source_commit);
    term(body, "Package SHA-256", &value.artifact.package_sha256);
    term(body, "Repository", &value.repository);
    term(body, "Target reference", &value.target_ref);
    term(body, "Expected base", &value.expected_base);
    term(body, "Policy", &value.policy);
    term(body, "Deployment target", &value.deployment_target);
    term(body, "Target revision", &value.target_revision);
    term(
        body,
        "Expected installed package SHA-256",
        &value.installed_package_sha256,
    );
}

pub(super) fn render(view: &StageView) -> String {
    let mut body = String::from(
        "<section id=\"protected-stages\"><h2>Protected release records</h2><p>Read-only committed records. A recorded check or grant is not a claim of current eligibility: the controller checks the exact subject, policy, expiry and action state when an authorized producer acts.</p><dl>",
    );
    term(
        &mut body,
        "Protected producer configuration",
        if view.configured {
            "configured"
        } else {
            "unconfigured"
        },
    );
    term(
        &mut body,
        "Current policy",
        view.policy.as_deref().unwrap_or("Not configured"),
    );
    term(
        &mut body,
        "Automatic rollout",
        if view.automatic_rollout {
            "reported enabled"
        } else {
            "disabled"
        },
    );
    body.push_str("</dl>");
    if !view.configured {
        body.push_str("<p class=\"notice\">Protected producers are not configured. This page cannot authorize checks, repository changes or rollout.</p>");
    }
    body.push_str("<h3>Checker and human records</h3>");
    if view.records.is_empty() {
        body.push_str("<p>No protected check or human grant records.</p>");
    }
    for record in &view.records {
        body.push_str("<article><h4>");
        body.push_str(match &record.command {
            Command::Check(_) => "Recorded checker evidence",
            Command::Grant { .. } => "Recorded human rollout grant",
            _ => "Other protected record",
        });
        body.push_str("</h4><dl>");
        term(&mut body, "Operation", &record.operation_id);
        term(&mut body, "Producer", &record.producer_id);
        match &record.command {
            Command::Check(check) => {
                subject(&mut body, &check.subject);
                term(&mut body, "Recipe SHA-256", &check.recipe_sha256);
                term(
                    &mut body,
                    "Checked at (Unix milliseconds)",
                    &check.checked_at_ms.to_string(),
                );
                term(
                    &mut body,
                    "Valid until (Unix milliseconds)",
                    &check.valid_until_ms.to_string(),
                );
                term(
                    &mut body,
                    "Checker cleanup verified",
                    if check.cleanup_verified { "yes" } else { "no" },
                );
                body.push_str("</dl><table><caption>Recorded individual check results</caption><thead><tr><th scope=\"col\">Check</th><th scope=\"col\">Result</th></tr></thead><tbody>");
                for (name, passed) in &check.results {
                    let _ = write!(
                        body,
                        "<tr><th scope=\"row\">{}</th><td>{}</td></tr>",
                        escape(name),
                        if *passed { "passed" } else { "failed" }
                    );
                }
                body.push_str("</tbody></table><p>These are recorded results, not an assertion that the check is still current.</p><dl>");
            }
            Command::Grant {
                subject: value,
                valid_until_ms,
            } => {
                subject(&mut body, value);
                term(
                    &mut body,
                    "Valid until (Unix milliseconds)",
                    &valid_until_ms.to_string(),
                );
            }
            _ => term(
                &mut body,
                "Record interpretation",
                "No authority is inferred from this record kind",
            ),
        }
        body.push_str("</dl></article>");
    }
    body.push_str("<h3>Repository and rollout actions</h3>");
    if view.actions.is_empty() {
        body.push_str(
            "<p>No recorded repository or rollout actions. No deployment outcome is inferred.</p>",
        );
    }
    for action in &view.actions {
        body.push_str("<article><h4>");
        body.push_str(match action.kind {
            Kind::Merge => "Repository merge",
            Kind::Rollout => "Human-authorized rollout",
        });
        body.push_str("</h4><dl>");
        term(&mut body, "Action", &action.id);
        term(
            &mut body,
            "Dispatch eligibility expiry (Unix milliseconds)",
            &action
                .dispatch_until_ms
                .map(|value| value.to_string())
                .unwrap_or_else(|| "Not recorded".into()),
        );
        term(
            &mut body,
            "State",
            match action.state {
                State::Prepared => "prepared: execution not established",
                State::Started => "started: completion not established",
                State::Applied => "applied: recorded observation",
                State::Refused => "refused: no success claimed",
                State::Unknown => "unknown: reconciliation required; no success claimed",
            },
        );
        subject(&mut body, &action.subject);
        term(&mut body, "Check operation", &action.check_id);
        term(
            &mut body,
            "Human grant operation",
            action.grant_id.as_deref().unwrap_or("Not recorded"),
        );
        term(
            &mut body,
            "Recorded observation",
            action
                .observation
                .as_deref()
                .unwrap_or("No observation recorded"),
        );
        body.push_str("</dl></article>");
    }
    body.push_str("<p>This browser view has no checker, merge or rollout controls. Ordinary task output cannot create these protected records.</p></section>");
    body
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        protected_release::{Action, Check, StageRecord},
        protocol::*,
    };
    use std::collections::BTreeMap;
    fn view() -> StageView {
        let candidate = ReleaseCandidate {
            id: "release-1".into(),
            task_id: "task-1".into(),
            task_revision: 1,
            accepted_attempt_id: "attempt-1".into(),
            revision: 1,
            checkpoint: Checkpoint {
                registration: crate::repository_catalogue::Revision {
                    repository_id: "display-fixture".into(),
                    registration_revision: "r1".into(),
                },
                repository: "origin".into(),
                commit: "a".repeat(40),
                remote_ref: "refs/heads/task".into(),
                operation_id: "publication".into(),
            },
            artifact: ReleaseArtifact {
                format: "borg-v2-release/1".into(),
                package_sha256: "b".repeat(64),
                package_bytes: 1024,
                manifest_sha256: "c".repeat(64),
                source_commit: "a".repeat(40),
                source_tree: "d".repeat(40),
                object_format: "sha1".into(),
                binary_sha256: "e".repeat(64),
                binary_bytes: 512,
                target: "x86_64-unknown-linux-gnu".into(),
            },
            qualification: ReleaseQualification {
                artifact: ArtifactQualification::Claimed,
                repository_submission: ProtectedStatus::Unconfigured,
                checks: ProtectedStatus::Unconfigured,
                human_rollout: ProtectedStatus::Unconfigured,
            },
        };
        StageView {
            candidate,
            configured: false,
            policy: None,
            records: vec![],
            actions: vec![],
            automatic_rollout: false,
        }
    }
    fn subject(view: &StageView) -> Subject {
        Subject {
            release_id: view.candidate.id.clone(),
            artifact: view.candidate.artifact.clone(),
            repository: "repo".into(),
            target_ref: "refs/heads/main".into(),
            expected_base: "f".repeat(40),
            policy: "policy-1".into(),
            deployment_target: "host-group".into(),
            target_revision: "target-1".into(),
            installed_package_sha256: "1".repeat(64),
        }
    }
    #[test]
    fn protected_record_view_keeps_absence_expiry_failure_and_unknown_distinct() {
        let mut view = view();
        let html = render(&view);
        assert!(html.contains("unconfigured"));
        assert!(html.contains("No protected check or human grant records"));
        assert!(html.contains("No recorded repository or rollout actions"));
        view.configured = true;
        view.policy = Some("policy-2".into());
        let subject = subject(&view);
        view.records = vec![
            StageRecord {
                operation_id: "check-1".into(),
                producer_id: "checker-1".into(),
                command: Command::Check(Check {
                    subject: subject.clone(),
                    recipe_sha256: "2".repeat(64),
                    checked_at_ms: 1,
                    valid_until_ms: 2,
                    results: BTreeMap::from([("tests".into(), true), ("recovery".into(), false)]),
                    cleanup_verified: false,
                }),
            },
            StageRecord {
                operation_id: "grant-1".into(),
                producer_id: "human-1".into(),
                command: Command::Grant {
                    subject: subject.clone(),
                    valid_until_ms: 3,
                },
            },
        ];
        for (index, state) in [
            State::Prepared,
            State::Started,
            State::Applied,
            State::Refused,
            State::Unknown,
        ]
        .into_iter()
        .enumerate()
        {
            view.actions.push(Action {
                id: format!("action-{index}"),
                kind: if index % 2 == 0 {
                    Kind::Merge
                } else {
                    Kind::Rollout
                },
                subject: subject.clone(),
                check_id: "check-1".into(),
                grant_id: Some("grant-1".into()),
                dispatch_until_ms: Some(3),
                state,
                observation: Some("recorded observation".into()),
            });
        }
        let html = render(&view);
        for expected in [
            "Current policy</dt><dd>policy-2",
            "Policy</dt><dd>policy-1",
            "Checker cleanup verified</dt><dd>no",
            "<td>failed</td>",
            "<td>passed</td>",
            "Valid until (Unix milliseconds)</dt><dd>2",
            "Recorded human rollout grant",
            "prepared: execution not established",
            "started: completion not established",
            "applied: recorded observation",
            "refused: no success claimed",
            "unknown: reconciliation required; no success claimed",
            "not a claim of current eligibility",
            "Automatic rollout</dt><dd>disabled",
        ] {
            assert!(html.contains(expected), "{expected}");
        }
        for forbidden in [
            "<form",
            "<button",
            "Roll out now",
            "Approve",
            "safe to deploy",
        ] {
            assert!(!html.contains(forbidden), "{forbidden}");
        }
    }
    #[test]
    fn protected_record_text_and_unknown_command_cannot_become_authority_or_markup() {
        let mut view = view();
        view.configured = true;
        view.policy = Some("<script>policy</script>".into());
        let mut subject = subject(&view);
        subject.repository = "javascript:<img src=x>".into();
        subject.deployment_target = "<b>target</b>".into();
        view.records = vec![
            StageRecord {
                operation_id: "<i>op</i>".into(),
                producer_id: "<script>producer</script>".into(),
                command: Command::Check(Check {
                    subject: subject.clone(),
                    recipe_sha256: "<recipe>".into(),
                    checked_at_ms: 1,
                    valid_until_ms: 2,
                    results: BTreeMap::from([("<img src=x>".into(), true)]),
                    cleanup_verified: true,
                }),
            },
            StageRecord {
                operation_id: "unknown".into(),
                producer_id: "producer".into(),
                command: Command::ExecuteMerge {
                    action_id: "must-not-run".into(),
                },
            },
        ];
        view.actions.push(Action {
            id: "<action>".into(),
            kind: Kind::Rollout,
            subject,
            check_id: "<check>".into(),
            grant_id: None,
            dispatch_until_ms: None,
            state: State::Unknown,
            observation: Some("<script>observed</script> & unknown".into()),
        });
        let html = render(&view);
        assert!(html.contains("&lt;script&gt;producer&lt;/script&gt;"));
        assert!(html.contains("&lt;img src=x&gt;"));
        assert!(html.contains("&lt;script&gt;observed&lt;/script&gt; &amp; unknown"));
        assert!(html.contains("No authority is inferred from this record kind"));
        for forbidden in [
            "<script>",
            "<img",
            "<b>target",
            "href=\"javascript:",
            "<form",
            "<button",
        ] {
            assert!(!html.contains(forbidden), "{forbidden}");
        }
    }
}
