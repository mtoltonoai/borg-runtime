# Core: Timers

> **CORE SPECIFICATION.** Behavior and invariants that hold under every outcome of every decision, free of implementation detail. This document defines timers as a native primitive: durable deferred events to a session's own inbox, fired exactly once by its partition, guarded so that a wake makes no model call when there is no work, bounded under empty wakes, and protected against clock faults. Requirements realize [Core Principle II](../../constitution.md) and [Platform Principle P5](../../constitution.md) and trace to [overview section 11](../overview.md).
>
> The capitalized key words are normative, with the meaning the constitution gives them. Each requirement is a single self-contained sentence carrying exactly one obligation, under a stable heading. Minimum intervals, per-session caps, the note size, and the clock-skew bound are declared defaults.

## Purpose And Scope

A timer is a deferred event to its own session's inbox. This document fixes schedules, identity,
exactly-once delivery, guards that avoid empty model wakes, backoff and governance, ownership, storage,
and clock-fault handling.

## Schedules

### Once Or Recurring

A timer MUST be either a one-shot at an absolute time or after a duration, or recurring at an interval from an anchor or on a calendar schedule in a time zone identified by name.

A due time MUST be stored as an absolute instant from the partition owner's wall clock.

A timer MUST NOT fire before its due time by the owner's clock.

A recurring timer MUST compute its next occurrence from its anchor or its last due time, so that a late
fire does not drift it and a clock step cannot replay one.

A calendar occurrence at a local time that does not exist MUST move forward by the gap.

A calendar occurrence at a local time that occurs twice MUST take the first.

An interval schedule MUST measure elapsed time.

A calendar schedule's shortest gap over the configured lead time MUST meet the minimum interval for its guard kind.

A timer whose schedule fails that check MUST be refused.

A timer's first occurrence MUST lie within the configured lead time.

### Flex And Missed Occurrences

A timer MAY carry a flex that bounds how late an occurrence may be delivered so that it shares a wake.

A definition's schedules MUST spread their fires over their flex by a hash of the session.

An occurrence missed during an outage MUST be delivered late unless a stale limit drops it.

A recurring timer's missed occurrences MUST collapse into one fire that carries their count.

## Identity

### A Timer's Name Is Unique Within Its Session

A timer's name MUST be unique within its session.

Setting a timer that differs from an existing one of the same name MUST replace it and report the previous next fire time.

A timer set without a name MUST derive one from its schedule, note, guard, and end condition, and not from its flex or priority.

A timer MUST carry a note within the configured size, returned with each fire.

Cancel MUST remove a timer or a name prefix.

Re-arm MUST move a timer's next occurrence.

List MUST show every timer with its owner and counts.

A timer name MUST follow the configured grammar.

The prefixes reserved for definition schedules and derived names MUST be refused for other timers.

Setting a timer identical to an existing one MUST change nothing and drop no fire.

## Delivery

### A Fire Is Exactly Once

A fire MUST append an event to the session's inbox, append the session to the partition's doorbell, and
advance the timer, in one commit under the partition's fence.

A fire's id MUST identify the timer, the setting of the timer that produced it, and the occurrence.

A retried append of a fire MUST be dropped as a duplicate.

A fire whose timer was cancelled, re-armed, or replaced before the session applies it MUST be dropped.

A fire MUST carry its timer's priority, so that it interrupts or waits like any notification.

A turn a fire starts MUST identify the fire as its cause.

A partition MUST NOT fire a timer of a session that is paused, blocked, or quarantined.

Occurrences held while a session was paused, blocked, or quarantined MUST follow the missed-occurrence rule when the hold clears, with the guard evaluated.

A timer MUST hold at most one undelivered fire.

A later passing occurrence MUST merge into the waiting fire's count.

Every occurrence MUST count toward a timer's count, so that the end deadline is the due time of the count-th occurrence.

A fire held by the wake budget or by a hold MUST NOT be recorded as a skip.

A fire's priority MUST be the session's base priority unless the timer asks for a lower one.

A timer MUST record the access scope of the turn that set it.

A timer's fires MUST carry that scope.

## Guards

### A Guard Prevents A Model Wake When There Is No Work

A timer MAY carry up to the configured number of guard conditions, each of which has to hold for a fire to wake the model.

A guard condition MUST be one of events held for delivery on given topics, no event on given topics since the timer's last evaluated occurrence, which is a position in the session's commit order, a slot key's value or a write to it, or one decision hook from the loaded definition.

A local guard condition MUST be checked before any decision hook, so that a false guard causes no model call, no session load, and no unfreezing of a frozen session.

A one-shot timer whose guard is false MUST end or wait rather than wake the model.

A recurring timer whose guard is false MUST skip the occurrence.

A fire that waited MUST be checked against its guard again when it is applied.

A once timer with a quiet guard whose event arrives MUST end as satisfied, with the satisfying event annotated.

## Backoff And Governance

### Empty Wakes Are Bounded

After consecutive empty wakes, a recurring timer's interval MUST lengthen up to the configured bound.

A timer set in a turn that a timer caused MUST inherit that timer's consecutive empty-wake count.

An unguarded recurring timer MUST NOT repeat more often than its configured minimum interval.

A session MUST hold no more than the configured number of its own timers.

Timer-caused empty wakes MUST be capped per session at the model-request hook, counted over the configured window that opens at the session's first counted wake.

A run that carries any event that is not a timer fire MUST NOT be held by the cap.

When the cap holds a run, the session MUST stay idle rather than blocked, with no model call made and the fire waiting in its inbox.

The window's reset MUST be a durable deadline at which a held fire wakes the session.

A wake MUST count as acting only when the session makes a side-effecting tool call, a send, or a publish.

A configuration change that lowers a timer limit below a session's current count MUST refuse new timers.

A configuration change that lowers a timer limit below a session's current count MUST NOT delete existing timers.

Adding a schedule, shortening a schedule's interval, removing a guard, or raising a schedule's priority MUST be classified as a widening change.

Removing a schedule or adding a guard MUST be classified as a narrowing change.

## Ownership

### A Session Owns Its Own Timers

A session MUST be able to set timers on itself.

A parent MUST be able to set timers on its sub-sessions.

A timer set on a session by another principal MUST require a grant, pass the control-operation hook, and
arrive through the target's inbox, so that the target's partition is the only writer of its timers.

A timer set by another principal MUST be owned and cancelled by its setter.

A timer set by another principal MUST count against a per-owner limit.

A timer MUST NOT be set across tenants.

The note of a timer another principal set MUST pass the notification-arrival deciders when its fire is first considered.

A session's timers MUST end with the session.

A definition's schedule MUST reach a non-resident session without waking it.

A changed definition schedule whose timing is unchanged MUST keep its count and its consecutive-empty-wake count.

A session MUST NOT cancel, re-arm, or replace its definition's schedules.

## Storage And Deadlines

### Timers Add No Keys Of Their Own

A session's timers MUST stay schedulable, without durable-store keys of their own, while the session is evicted or frozen.

Which timer fields are stored unencrypted, so that the partition schedules, skips, and recovers without the session's keys, and which are encrypted under the session's keys is pinned by spec/contracts/stored-form.md.

### One Deadline Set Per Partition

A partition MUST track the earliest durable deadline across its sessions' timers and the platform's own deadlines.

The partition's sleep on the earlier of its doorbell watch and its earliest deadline is specified in spec/core/sessions-and-partitions.md.

A new owner MUST rebuild the partition's deadlines from one consistent read of its durable state.

A process-scoped timer that stops when its process stops MUST NOT be durable.

## Clock Faults

### A Partition Detects A Faulty Clock

A wall-clock step of more than the configured bound MUST be an event, detected within one doorbell renewal.

An owner whose clock differs from the store's commit timestamps by more than the configured bound MUST hold its timer fires and alert.

## Observability

### Every Timer Event Is Observable

Every set, rejection, guard check, skip, fire, delivery, drop, outcome, and end MUST be an event.

Timer metrics MUST split only by owner kind, schedule kind, condition kind, delivery, residency, and
reason.
