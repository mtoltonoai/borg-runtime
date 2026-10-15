# Core: Failure Handling

> **CORE SPECIFICATION.** Behavior and invariants that hold under every outcome of every decision, free
> of implementation detail. This document defines the one handling each failure class has and the state a
> session ends in, the deadlines and budgets that bound every retry, quarantine, and the rule that every
> blocked or quarantined state has a defined exit. Requirements realize
> [Platform Principle P6](../../constitution.md) and trace to [overview section 14](../overview.md).
>
> The capitalized key words are normative, with the meaning the constitution gives them. Each requirement is a single self-contained sentence carrying exactly
> one obligation, under a stable heading. Retry budgets, backoff schedules, and attempt counts are
> declared defaults.

## Purpose And Scope

The durable state machine is the authoritative record, all work is re-drivable from it, and every side effect carries an idempotency key. Given that, each failure a dependency can present has exactly one handling,
so the system's behavior under failure is fully enumerated rather than decided case by case. This document enumerates those handlings.
Who owns the list of root sessions is a fixed default, imperative, recorded in [spec/record.md](../record.md) part A; its declarative alternative is the entry "Declarative Root Sessions And A Reference Definition Source" in [spec/decisions/deferred.md](../decisions/deferred.md), and the requirements under the default and under each alternative, including the handling of a definition source that fails, are in spec/decisions/session-management/.

## Prerequisites

### Every Side Effect Can Be Redone

Every side effect carries an idempotency key, as Platform Principle P6 in constitution.md requires; which key each kind of side effect carries is fixed here.

A model attempt's idempotency key MUST be its attempt id.

A tool call's idempotency key MUST be its call id.

### Every Unit Of Work Has A Deadline

Every model turn MUST have a deadline.

Every tool call MUST have a deadline.

A deadline enforced by a remote host MUST NOT end the work before the deadline in the issuer's time.

A deadline enforced by a remote host MUST end the work within the configured bound after the deadline.

### Every Retry Has A Budget

Every retry MUST be bounded by a budget.

Exhausting a retry budget MUST end in a defined blocked state that carries the reason.

## The Failure Table

### A Throttled Or Unavailable Model

A model attempt that is throttled, overloaded, timed out, or failed internally MUST be retried with
backoff and jitter within the turn's attempt budget.

A retry of a throttled model attempt MUST honor the provider's retry-after when one is given.

The platform MUST keep a circuit breaker per model and endpoint.

A session whose turn exhausts its attempt budget against a model MUST end blocked with the reason.

### A Context Window Exceeded

A model attempt that exceeds the context window MUST be classified as such rather than retried unchanged.

What follows that classification depends on the execution decision; the requirements under each outcome are in spec/decisions/execution/.

### An Invalid Request

A model attempt rejected as invalid MUST NOT be retried.

A session whose request was rejected as invalid MUST end blocked, with an event carrying the reason.

### A Refusal, A Malformed Reply, Or A Truncated Reply

A model refusal or a provider's content filter MUST be handled as a policy block.

A model reply that fails a declared output format MUST be retried with the specific validation error, within the configured attempts.

A model reply cut at the output cap MUST be continued by a further turn from the point of truncation.

### An Authentication Failure Toward A Dependency

An authentication failure toward a dependency MUST be handled once per cluster, with backoff and an
alert, rather than once per session.

A tenant-supplied role that cannot be assumed MUST block the tenant's affected sessions with the reason.

A refused role MUST NOT be tried again before the configured bound elapses or the tenant's configuration changes.

### A Policy Block

A request blocked by policy MUST NOT be resent unchanged.

A session whose request was blocked by policy MUST take the handling the session's policy decides.

### A Decider Or Its Dependency Fails

A decider that fails at the notification-arrival hook MUST leave the event unapplied in the inbox and block the session with the reason.

A held event MUST be evaluated again on resume or on the session's next wake.

Each failed evaluation of a held event MUST count as one attempt toward the configured attempt budget for an event that keeps failing.

A failure of a decider's dependency rather than of its decision MUST NOT count toward that attempt budget.

A failed decider dependency MUST be retried with backoff and an alert once per destination rather than once per session.

A session blocked because a decider's dependency failed MUST retry with backoff when the dependency recovers, without an adapter's intervention.

### A Failure Mid-Stream

What is kept of a model reply that fails mid-stream depends on the execution decision; the requirements under each outcome are in spec/decisions/execution/.

A retry after a reply that failed mid-stream MUST be a new attempt with its own attempt id.

### An Unreachable Or Timed-Out Tool

An idempotent tool call whose server is unreachable or timed out MUST be retried within its budget.

A side-effecting tool call whose server is unreachable or timed out MUST return an unknown outcome to
the model.

### A Tool Error

An error a tool returns reaches the model as an error result rather than as a failed turn; spec/contracts/tool-call.md pins that handling.

### A Store Conflict Or Outage

A durable-store transaction that conflicts or fails MUST be retried with backoff.

A failed store transaction MUST leave the session's state unchanged.

A durable-store error met while checking the partition's fence MUST be handled as a store failure to retry rather than as a superseded fence.

### State That Cannot Be Decoded

A session whose state cannot be decoded MUST be quarantined.

A quarantined session's state MUST NOT be overwritten.

A quarantined session MUST NOT stop its partition from serving its other sessions.

A frozen range whose integrity check fails when it is loaded MUST be treated as state that cannot be decoded.

A quarantined frozen session MUST NOT be loaded again before a resume, so that one failed blob raises one event.

A queued message or a timer fire whose stored form cannot be decoded or unsealed MUST be set aside with an event that carries the message's and the reader's format identities rather than stall its queue.

A resume of a session quarantined for undecodable state MUST retry the decode and lift the quarantine only if the decode succeeds.

### An Event That Fails Repeatedly

An event that fails the configured number of attempts MUST be quarantined so that it cannot stall its partition.

A session whose event was quarantined MUST end blocked with the reason.

A resume of a session quarantined for an event that keeps failing MUST either retry the event with a fresh attempt count or skip it with an audit event.

A configuration change applied to a session quarantined for an event that keeps failing MUST reset the event's attempt count and re-drive the event.

A pause, stop, or quarantine imposed by an emergency control MUST NOT be lifted by a configuration change.

An owner that re-drives a session's first unapplied event MUST commit the attempt count for that event before it runs the attempt.

A change that a change-feed handler rejects permanently goes to a dead-letter record with an alarm, and the cursor moves past it, as spec/core/shared-framework.md specifies.

### An Owner That Crashes Or Moves

The new owner of a partition MUST resume each session from its committed cursors.

A superseded owner is fenced from committing, as spec/core/sessions-and-partitions.md specifies.

## Every Blocked State Has An Exit

### Every Blocked Or Quarantined State Has An Exit

Every blocked or quarantined state MUST have an exit transition that an adapter can trigger through an inbox event or the resume control.

Alarming an operator MUST NOT be the only handling of any failure class.

A turn that the budget check denies MUST wait, with its session idle rather than blocked, while the budget does not allow it.

### Quarantine And Blocks Are Events

Every quarantine MUST be published as an event with its reason.

Every transition to a blocked state MUST be published as an event with its reason.

A blocked state caused by a decider MUST identify the decider and state whether the decider answered a block or failed to answer.
