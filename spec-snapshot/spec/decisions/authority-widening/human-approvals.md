# Outcome: Human Approvals

> **OUTCOME SPECIFICATION.** These requirements apply only when the deferred entry "Human Approvals And Record-Based Autonomy" in [spec/decisions/deferred.md](../deferred.md) is reopened and this outcome adopted, alone or together with record-based autonomy; they add to the core and never replace it. Requirements trace to
> [overview section 6](../../overview.md) and [overview section 19](../../overview.md).
>
> The capitalized key words are normative, with the meaning the constitution gives them. Each requirement is a single self-contained sentence carrying exactly
> one obligation, under a stable heading.

## Purpose And Scope

The core widens authority only by a sound checker, and a decision reaches a person only when no mechanism
can decide it. Under this outcome such a decision does reach a person, and this document fixes how the
request is presented, who may not receive it, and how the people who decide are measured. The mechanism
that routes a decision, the hook points, and the decision kinds are in
[core/governance.md](../../core/governance.md).

## Requests That Reach A Person

### A Request That Reaches A Person Is Batched And Measured

A request that reaches a human MUST be batched.

A batch of requests that reaches a human MUST carry an evidence summary.

An approval of an agent-authored change that reaches a human MUST NOT go to the operator who directed the work.

A human approver MUST be measured by time to approve and approval rate.

Seeded known-bad requests MUST reach approvers at a configured rate, so that an approver who is not
genuinely reviewing is detectable.

### A Defer To People Lists A Resolver

A defer whose allowed resolvers are people MUST also list an automated resolver that is a sound checker, unless the action is human-only.

The automated resolver of a defer MUST act only when the resolver check leaves no eligible person.

A defer on a human-only action MUST have no automated resolver.

A defer on a human-only action MUST have block as its timeout outcome.

### An Answer Becomes A Grant

A consent to use the operator's own authority MUST go to that operator.

An operator's answer that widens a session's authority MUST be recorded as a narrow, short-lived grant rather than applied as a one-time allow.

A refusal for a missing scope MUST reach the model as a request for approval that identifies the scope rather than as a plain error.
