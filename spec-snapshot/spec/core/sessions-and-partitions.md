# Core: Sessions And Partitions

> **CORE SPECIFICATION.** Behavior and invariants that hold under every outcome of every decision, free
> of implementation detail. This document defines what a session is, where each session comes from and
> who supervises it, how sessions are owned and placed in partitions, how a partition wakes, reads, and
> commits, how an inbox is applied exactly once, how a running session is reconfigured, and how the
> durable layout keeps cost proportional to the number of active sessions. The fan-out of an event to many inboxes is in spec/core/events-and-subscriptions.md. Requirements realize
> [Platform Principle P5](../../constitution.md), [Platform Principle P3](../../constitution.md), and
> [Core Principle II](../../constitution.md) and trace to [overview section 3](../overview.md),
> [overview section 4](../overview.md), and [overview section 10](../overview.md).
>
> The capitalized key words are normative, with the meaning the constitution gives them. Each requirement is a single self-contained sentence carrying exactly
> one obligation, under a stable heading. The partition bit count, the
> per-session key count, the transcript head bound, and the inbox cap are declared defaults.

## Purpose And Scope

This document fixes the unit the platform schedules and persists (the session), the unit that owns it
(the partition), and the discipline that makes ownership safe under crashes, moves, and loss of connectivity between nodes.
It states invariants: exactly once, in order, fenced, and re-drivable. The failure table that states the handling for each dependency failure is in spec/core/failure-handling.md. How a session's model turns run is settled by decision [execution](../decisions/execution.md); the requirements under each outcome are in spec/decisions/execution/.
Who owns the list of root sessions is a fixed default, imperative, recorded in [spec/record.md](../record.md) part A; its declarative alternative is the entry "Declarative Root Sessions And A Reference Definition Source" in [spec/decisions/deferred.md](../decisions/deferred.md), and the requirements under the default and under each alternative are in spec/decisions/session-management/.

## Sessions

### A Session Is One Continuous Transcript

A session MUST have one continuous transcript for its lifetime.

The platform MUST NOT bound a session's lifetime.

Which root sessions exist MUST be decided by their owner, through the mechanism of the adopted session-management outcome, rather than by the core.

An idle duration a tenant configures is tenant policy rather than a bound the platform places on a session's lifetime.

### A Memory Belongs To An Agent Identity

A memory MUST be keyed by agent identity rather than by session.

An agent's memories MUST persist across every session of that agent identity.

A memory MUST be recordable by a mechanism that observes the session rather than only by a tool call the agent makes.

A memory recorded by observation MUST carry the access scope of the transcript range it was derived from.

The loss of one host MUST NOT destroy a memory.

### Starting And Ending Are Low-Cost

Ending a session MUST publish its final observation record.

Ending a session MUST freeze its transcript into the blob store.

Ending a session MUST release its partition resources.

A session that has ended MUST refuse every operation except its removal, with a stable code.

A publish or send to a session that has ended MUST write nothing.

### Removing A Session

A tenant's principal MUST be able to remove a session it owns.

A removal of a session at rest MUST be accepted, whether the session is idle, blocked, or ended.

A removal of a session that has not ended MUST end the session before its record is removed, so that its transcript is frozen first.

A removal MUST take everything the durable store holds for the session out of it in one commit.

A read of a removed session MUST answer not found.

### A Session Id Encodes No Location

A session id MUST be unique across tenants.

A session id MUST NOT encode where the session runs.

A keyed instance's session id MUST be derived deterministically, beginning with the tenant, from its template, its instancing form, and its publisher-scoped instance key.

The instance key of a template keyed by a publisher MUST be the pair of the publisher's authenticated principal and the key.

### A Recurring Id Has Incarnations

A session MUST be identified by its session id together with an incarnation.

An ended session whose id can recur MUST leave a tombstone carrying its last lease generation and tail position.

A new incarnation of a session id MUST start its lease generation and its tail position above the values its tombstone carries.

A call id MUST be unique across every incarnation of its session id.

Ending an incarnation MUST close each call it leaves open.

An end that leaves calls open MUST leave a pending-close record from which a close and a reconcile are resent for each call while its outcome is unknown and its cutoff has not passed.

## Session Origins

### Every Session Records Its Origin

Every session MUST record its origin as one of a caller, a definition, a keyed template, or a parent session.

A session's origin MUST identify the party responsible for its existence.

A tenant's principal MUST be able to create a root session.

A tenant's principal MUST be able to end a root session it owns.

### Platform-Managed Sessions Are Supervised

A sub-session or a fork MUST be owned by its parent.

A parent MUST be able to list its children.

A parent MUST be able to stop its children.

A child MUST end with its parent unless the call that started it specified otherwise.

A keyed template MUST be either a definition or a template a caller registered.

A partition MUST create a keyed instance on the first event addressed to its key.

A keyed instance MUST be owned by its template.

A keyed template MUST carry an instance cap.

A first event that would exceed the instance cap MUST be refused with a reason event.

An event addressed to a template that has no configuration in force, or that has been retired, MUST NOT create a session.

An event addressed to a template that has no configuration in force, or that has been retired, MUST be dropped with a reason event.

### Liveness Is Decided By A Mechanism

Whether a session that should be running is running MUST be decided by a mechanism rather than by a person checking.

The platform MUST deliver a session's lifecycle events to the owner of the list of root sessions.

The owner of the list of root sessions MUST NOT need to poll the platform to learn a session's state.

## Partitions

### The Partition Owner Is The Single Writer

Every write to a session's state MUST be made by the owner of the session's partition.

A partition MUST have exactly one owner at a time.

### A Session's Partition Is A Function Of Its Id

A session's partition MUST be determined by the leading bits of a hash of its session id, with the bit
count taken from configuration.

Moving a partition between nodes MUST NOT move a session's durable data.

The hash that maps a session id to its partition MUST be part of the durable format, so that changing it is a migration.

A split of the partition space MUST move no session's durable data.

### The Space Splits By Doubling

Growing the partition space by one bit MUST split each partition into exactly two children.

A session MUST move only from a partition to one of that partition's two children.

### Placement Comes From A Coordinator

Partitions MUST be assigned to nodes by one coordinator per partitioned set, from one consistent view of
membership.

Adding a node MUST move only the partitions the placement policy gives to it.

Removing a node MUST move only the partitions it owned.

The assignment table MUST be durable.

The assignment table MUST record each partition's node and generation.

A node MUST start the partitions it is assigned with the table's generation as their instance
generation.

A node MUST drain the partitions reassigned away from it.

A coordinator restart MUST NOT stop a running partition.

A node the failure detector declares dead MUST have its partitions reassigned.

A planned drain MUST be an input to the placement policy.

A draining node MUST release its partitions to the coordinator before it exits rather than wait for its membership lease to lapse.

An orphaned or never-started partition MUST be placed at once.

The coordinator MUST check its own generation in the same commit as each batch of moves.

A coordinator whose membership view has lost more than the configured fraction of the current owners MUST hold its moves while the view stays that way, within the configured bound.

A new coordinator's first pass MUST wait for a fresh membership view.

A node that reconnects MUST NOT take a partition over from a host it believes dead before its own membership view has settled.

A node MUST start, after the configured grace, any partition assigned to it that is not running, fenced on the generation it observed.

### Ownership Is Fenced By Generation

Every partition commit MUST check the partition's generation.

A commit by a superseded owner MUST fail.

Correctness MUST NOT depend on a superseded owner detecting that it was superseded.

A wake addressed to a partition generation newer than the resident owner's MUST stop the resident owner and be handled by an owner at the new generation, so that the wake is not lost.

A session's lease generation MUST advance, in the same commit as the change, on every change of the party that may act for the session: an owner move, a restore, a pause, a quarantine, a stop or end, and a resume.

A new owner MUST commit a session's lease-generation advance before it issues any recovery dispatch, reconcile, or close for that session.

A close sent for a call abandoned at a session's end MUST carry the end's advanced lease generation.

The acknowledgement of a session-scoped resume MUST follow the session's commit and carry the session's new lease generation.

### A Forwarded Call Carries The Caller

A call that one node forwards to the owner of another partition MUST carry the caller's verified principal and chain unchanged in a claim of its own.

The owner of the partition MUST decide a forwarded operation on the caller's verified principal and chain rather than on the identity of the node that forwarded it.

A forwarded call MUST be authenticated by an assertion of the forwarding node that a verifier checks against the keys of its own lineage only.

The identity a forwarding node presents MUST hold only the scope of the kind of hop it makes.

The audit record of a forwarded operation MUST carry both the forwarding node and the caller's verified principal and chain.

## Waking

### A Partition Sleeps On Its Doorbell And Its Deadlines

A partition MUST sleep on the earlier of a watch on its doorbell and its next deadline.

A partition MUST read the wall clock again on every wake.

A watch on a newly created partition's doorbell MUST block until the first append rather than return at once because nothing has been written.

A doorbell watch MUST be kept alive or renewed within the transport's idle bound.

### A Partition Reads At The Woken Version

A partition MUST read its doorbell and its dirty inboxes in one read-only transaction pinned at the
version its watch woke for.

A partition MUST NOT read its doorbell or inboxes with a stale read that can miss the append that woke it.

### A Commit Conflicts Only On The Fence

A partition's commit of state, cursors, and trims MUST be rejected for a conflict only when the partition's fence has changed.

## Applying An Inbox

### Each Event Applies Exactly Once, In Order

A publisher MUST append an event to the session's inbox and the session id to the partition's doorbell
in one transaction.

A partition MUST apply each session's inbox in order.

A partition MUST commit a session's new state and its cursor in one transaction.

Each inbox event MUST apply exactly once relative to the session's state.

A slow session MUST delay only itself.

A commit whose outcome is unknown MUST be resolved by re-reading the committed state under the fence before any redo.

A session MUST skip a topic delivery whose position is at or below the last position it applied or dropped from that topic within the same lineage epoch.

A session MUST NOT compare topic positions across lineage epochs.

### The Doorbell Advances To A Watermark

The doorbell cursor MUST advance to the position below which every session listed in the doorbell has applied every event.

Recovery MUST re-read the doorbell from the watermark.

A doorbell entry MUST be trimmed no earlier than the commit that records the entry's first-read effects.

### A Pending Inbox Is Bounded

The pending inbox of a session that is not draining MUST be bounded by the configured cap.

An inbox past its cap MUST keep, per topic, a marker that records the count and the first and last dropped events in place of further events.

A session whose inbox kept a dropped-count marker MUST re-sync from the topic when it drains.

An inbox MUST NOT drop an event silently.

Every inbox entry's payload, with its attachment references counted together, MUST be within the configured payload bound, whatever producer wrote it.

An inbox past its cap MUST still admit an answer to held work: a defer's resolution or timeout, a deferred call's outcome, and a tool result that matches an open dispatch.

## Work Runs Inside The Partition

### Long Work Commits As State Transitions

A model turn or tool run MUST be a task whose result the partition commits as a state transition.

A crash during a step MUST leave the durable state at the step in progress.

The next owner MUST re-drive a step left in progress.

## Residency

### The Resident Form Is A Cache

A partition MUST load a session when the session has work.

The order in which a partition evicts idle sessions when it needs memory is specified under the adopted outcome of decision execution, in spec/decisions/execution/.

Dropping a session's resident form MUST lose nothing.

A whole-session freeze MUST fail when a publish reaches the session's inbox during the freeze, so that the publish is applied rather than lost.

A session MUST freeze whole only while it has no work in progress, no tool call open, and its inbox applied.

## Reconfiguration

### A Change Applies At The Next Turn Boundary

A configuration change MUST apply to a running session at its next turn boundary.

When a revocation of a tool takes effect is specified in spec/core/tools-and-dispatch.md.

A configuration change MUST NOT apply between a tool call and the delivery of its result.

A session MUST check for a pending configuration change when it loads, as it does at a turn boundary.

A configuration change to a keyed template MUST cost a number of durable writes that does not depend on the number of its instances.

A configuration change MUST NOT wake an idle instance to adopt it.

A partition MUST NOT create a keyed instance while a stop holds at its template's or tenant's scope.

A partition MUST apply an emergency control to a non-resident session when it next loads, before it starts a turn.

### A Prefix Change Preserves The Cache

How a prompt or policy change reaches the model without rewriting the cached prefix, and the compaction that merges it into the system prompt, are specified under the adopted outcome of decision execution, in spec/decisions/execution/.

A field the model never sees MUST apply immediately.

## Durable Layout

### Cost Follows Active Sessions

An active session MUST occupy no more durable-store keys than the configured per-session key count.

A frozen session MUST occupy no durable-store key of its own.

The transcript head held in the durable store MUST be bounded by the configured size.

A queue in the durable store MUST be trimmed once its entries are applied and frozen.

The durable store's working set MUST grow with the number of active sessions rather than with the total
number of sessions.

A turn's commit MUST append the turn's new transcript bytes rather than rewrite the transcript.

A compaction MUST switch the session to the compacted transcript in one commit, however many commits wrote it.

A transcript log that no session points to MUST be deleted.
