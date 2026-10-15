# Contract: The Usage Record

> **CONTRACT.** This document pins the one record the platform writes for every model attempt, tool call,
> and decider call. Budget deciders, experiments that compare versions, and metering all read the
> same stream, so its shape is honored across releases. Its requirements realize
> [Platform Principle P7](../../constitution.md) and [Core Principle III](../../constitution.md) and
> trace to [overview section 13](../overview.md).
>
> The capitalized key words are normative, with the meaning the constitution gives them. Each requirement is a single self-contained sentence carrying exactly one obligation, under a stable
> heading. The retention period and the sink are declared defaults.

## Purpose And Scope

The usage stream is the one place the platform's cost and causes are visible together, so that a change to a definition can be evaluated on the same data its predecessor ran on. A record carries the principal, the on-behalf-of chain, the cause, and the cost, and never carries transcript content.

## One Record Per Unit Of Work

### Every Attempt Is Recorded Once

Exactly one usage record MUST exist for every model attempt.

Exactly one usage record MUST exist for every tool call.

Exactly one usage record MUST exist for every decider call.

A shadow decider call MUST have its own usage record marked as shadow.

A decision served from the decision cache MUST NOT produce a usage record, because no decider was called.

A model attempt interrupted before it completed MUST have its usage record written by the next owner, marked interrupted, with its token counts recorded as unknown.

## Fields

### A Record Carries Its Tenant And Actors

A usage record MUST carry its tenant.

A usage record MUST carry the session's principal chain.

A usage record MUST identify the session and its parent session, if any.

A usage record MUST carry the digest of the loaded definition the session ran under.

A usage record MUST carry the attribution tags the definition configured, such as a project or a task.

### A Model Attempt's Record Carries Its Cause And Outcome

A model attempt's record MUST state what caused the turn: an event, a timer, an operator message, or a
stop-admission continuation.

A model attempt's record MUST state the turn's outcome as no action, or the actions it took.

A model attempt's record MUST carry the cache reads and cache writes the attempt incurred.

A model attempt's record MUST carry the prompt size.

A model attempt's record MUST identify the model and the capacity source that served it.

### A Record Carries No Content

A usage record MUST NOT carry transcript content.

## Retention And Delivery

### The Stream Has Its Own Retention

Usage records MUST have a retention period of their own, separate from session data.

A partition MUST deliver its usage records to their sink within the configured interval.

A drain MUST flush pending usage records before the final session records are written.

## Additive Evolution

### Additive Evolution Of This Contract

A change to this contract MUST be additive with respect to deployed readers, or else carry an explicit
version increment.

A change to this contract that is not additive with respect to deployed readers MUST carry a stated
migration path.
