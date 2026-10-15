# Outcome: Direct Context, Compaction, And The Prompt Cache

> **OUTCOME SPECIFICATION.** These requirements apply only when the decision record adopts the direct outcome of decision [execution](../execution.md); they add to the core and never replace it. This document defines how the model's view of a transcript is derived from the durable log, how a transcript is compacted in the background and frozen into the blob store, and how the platform manages the prompt cache, the largest cost. Requirements realize [Platform Principle P2](../../../constitution.md) and [Platform Principle P7](../../../constitution.md) and trace to [overview section 18](../../overview.md).
>
> The capitalized key words are normative, with the meaning the constitution gives them. Each requirement is a single self-contained sentence carrying exactly one obligation, under a stable heading. Thresholds, lifetimes, and the compression scheme are declared defaults.

## Purpose And Scope

A transcript has no upper bound on its length, a prompt cache expires in idle gaps, and both cost tokens. This document
fixes the derived views (rollback, trimming, bounded results), background compaction, freezing, and the
cache policy, all configured per session from its definition.

## The Model's View Is Derived

### The Log Is Never Edited

The model's view of a transcript MUST be derived from the durable log.

The durable log MUST NOT be edited in place.

### Rollback Removes Attempts From The View Only

A rollback MUST remove failed attempts from the model's view only.

The durable log MUST keep rolled-back attempts for audit and observation.

Where a provider binds reasoning to the history it was produced in, a rollback MUST drop the bound
reasoning rather than send it with an edited history.

A rollback MUST remove a failed attempt from the model's view only where the tool's declared semantics allow it.

The durable log MUST keep a rolled-back attempt with the spans a blocking decider marked redacted.

A reasoning block that held a secret MUST be kept in the durable log only as the redaction marker.

The transcript and the durable log MUST record content as it took effect after transforms rather than as it arrived.

### Trimming Leaves A Resolvable Placeholder

A stale tool result trimmed from the view MUST be replaced by a placeholder the model can resolve by the
call id.

Trimming MUST evict the oldest stale results first.

### Tool Results Are Bounded In The View

A tool result enters the view as a bounded view whose bounds come from the tool spec, as spec/contracts/tool-call.md pins.

The bounded view of a tool result MUST be enforced before any model request, so that no single result can exceed the context window.

## Compaction

### Thresholds Are Per Session And Below The Context Window

A session's definition MUST set a soft threshold at which a background compaction starts.

A session's definition MUST set a hard threshold at which compaction becomes a precondition of the next
turn.

Both thresholds MUST be below the model's context window.

### Compaction Runs Beside The Turns

A compaction MUST cover the transcript up to a cut point.

A compaction result MUST be swapped in only at a turn boundary.

A session MUST continue to process turns while its compaction runs.

Cancelling a compaction MUST NOT change the session's durable state.

Cancelling a compaction MUST take effect by the next turn boundary.

An event that exceeds the session's interrupt threshold MUST preempt a running compaction, which is then
rescheduled.

A compaction's priority MUST rise as the context nears the hard threshold, so that a continuous stream of events cannot postpone it indefinitely.

A turn that arrives during a compaction MUST run on the uncompacted context.

A compaction MAY run inline within a step's own request where the provider compacts on a request-side threshold.

A session MUST ask for compaction at its soft threshold through whichever path the provider offers.

A compaction a provider returns inside a reply MUST be applied as a compaction, with the messages it covers archived, the transcript renumbered, and the compaction counted.

The answer carried beside such a compaction MUST be kept as the step's answer rather than the step run again.

### The Swap Preserves History

Swapping in a compaction MUST replace everything before the cut with the summary and keep the tail
verbatim.

The history before the cut MUST be frozen into the blob store rather than deleted.

The swap MUST apply pending changes to the cached prefix, such as tool and system prompt changes, in the
same rewrite.

The switch to a compacted transcript MUST take effect in one commit, however many commits wrote the compacted transcript.

### The Keep Set Stays Verbatim

A compaction MUST keep the recent tail verbatim.

A compaction MUST keep the keep set verbatim: plans, open items, unresolved defects, constraints,
decisions, and identifiers the work depends on.

The keep set MUST include the session's pending timers with their notes.

### Method

A compaction MUST use the provider's native compaction where the provider offers one.

The platform's own summarizer MUST be used only as a fallback.

A compaction MUST count against the session's budget.

A compaction MAY run in the batch latency class.

### A Charter Change Schedules A Compaction

A change to a session's prompt or policy MUST schedule a low-priority compaction that merges the change into the system prompt.

Charter changes MUST be debounced, so that a burst of them costs one compaction.

A compaction that follows a charter change MUST write its summary under the new charter.

A charter change arriving while a compaction is already due MUST be included in that compaction.

### Compaction Failures Are Bounded

A compaction that fails MUST be retried within the configured attempts.

A compaction a turn depended on that exhausts its attempts MUST block the session with the reason.

A compaction requested on its own that exhausts its attempts MUST leave the session idle with its transcript unchanged.

A compaction the provider answers with no summary MUST leave the transcript unchanged.

## Freezing

### Stable Ranges Move To The Blob Store

A transcript range that stabilizes, by passing a compaction boundary or a size threshold, MUST be frozen
into the blob store.

A frozen range MUST be encoded, compressed, and encrypted before it is written.

The durable log MUST keep only a pointer to a frozen range: the blob name, the range, and its sizes.

An idle session MUST be able to freeze entirely, leaving a pointer record.

Freezing triggers MUST be per-session configuration.

The duplicate guard MUST be carried in the frozen form and restored with the session.

### Frozen History Is Recoverable

A session's history MUST be recoverable from the blob store and the key service alone.

An observation record points at the same frozen blobs the transcript ranges refer to, as spec/core/observation-and-usage.md requires.

## The Transcript Cache

### Every Tier Above The Log Is A Cache

Dropping any tier of the transcript cache MUST lose nothing.

Plaintext derived from a tenant's content MUST NOT be written to a cache tier that outlives the process.

Plaintext derived from a tenant's content and held in a node's memory MUST expire within the decrypt cache's window.

## The Prompt Cache Policy

### The Policy Comes From The Definition

A session's cache policy MUST come from its definition.

### The Platform Manages The Lifetime

The platform MUST choose a session's cache lifetime per session, taking a longer lifetime where the model endpoint offers one and the session's wake history shows that the longer lifetime reduces cost.

The platform MUST keep a prefix warm across an expected short gap with a refresh read before it expires,
when the next wake is likely inside the window.

The platform MUST let a prefix expire when the next wake is not expected inside the window.

### The Platform Avoids Full Rewrites

The platform MUST compact before a long idle, so that the rewrite on the next wake is small.

A low-priority wake MAY be delayed and merged into the session's next wake, where its deadline allows, so that it runs while the cache is warm.

### Every Cache Setting Is Measured

Every usage record MUST carry the cache reads and writes of its attempt, so that the policy is measured
per session and per definition.

Every cache setting MUST be configuration, so that a change to it rolls out as a definition version.
