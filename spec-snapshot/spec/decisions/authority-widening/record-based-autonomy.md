# Outcome: Record-Based Autonomy

> **OUTCOME SPECIFICATION.** These requirements apply only when the deferred entry "Human Approvals And Record-Based Autonomy" in [spec/decisions/deferred.md](../deferred.md) is reopened and this outcome adopted, which adds to the human-approvals outcome; they add to the core and never replace it. Requirements trace to
> [overview section 6](../../overview.md) and [overview section 19](../../overview.md).
>
> The capitalized key words are normative, with the meaning the constitution gives them. Each requirement is a single self-contained sentence carrying exactly
> one obligation, under a stable heading.

## Purpose And Scope

Under this outcome, what a role may do widens as its record of changes without rollback grows and narrows
after a rollback, through a temporal policy the governance mechanism evaluates. This document fixes the two
directions of that policy; it applies together with [human-approvals.md](human-approvals.md), and the
mechanism that evaluates a temporal policy is in [core/governance.md](../../core/governance.md).

## Autonomy

### Autonomy Follows The Record

A temporal policy MAY widen what a role may do as its record of changes without rollback grows.

A temporal policy MUST narrow what a role may do after a rollback.
