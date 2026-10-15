# Outcome: Checkers Only

> **OUTCOME SPECIFICATION.** These requirements apply when the decision record ([spec/record.md](../../record.md) part A) keeps the fixed default checkers-only for how authority is widened; they add to the core and never replace it. This document states that authority is widened only by a sound checker and that no decision is routed to a person while agents run. Requirements trace to [overview section 6](../../overview.md) and [overview section 19](../../overview.md).
>
> The capitalized key words are normative, with the meaning the constitution gives them. Each requirement is a single self-contained sentence carrying exactly one obligation, under a stable heading.

## Purpose And Scope

Under this outcome no person approves an individual action at runtime. A person acts in advance, by approving a gate spec once and by changing policy, and a sound checker decides each instance. A policy that would otherwise defer a decision to a person blocks instead, so the platform runs without an approval queue. The governance mechanism that holds under every outcome is in [core/governance.md](../../core/governance.md).

## Widening By Checker

### Only A Sound Checker Widens Authority

Authority MUST be widened only by a sound checker.

### A Deferral To A Person Blocks Instead

A hook whose policy would defer a decision to a person MUST block instead.

A decision request MUST NOT be routed to a person at runtime.

## People Act Through Policy

### A Gate Spec Is Approved By A Person Other Than The Author

A gate spec MUST be approved by a person who is not an author of the changes it governs.

### A Person Changes Policy, Not Instances

A person MUST change what agents may do only by changing policy through the control-operation hook.
