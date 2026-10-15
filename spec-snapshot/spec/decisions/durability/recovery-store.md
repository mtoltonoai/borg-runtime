# Outcome: Recovery Store

> **OUTCOME SPECIFICATION.** These requirements apply only when the deferred entry "A Recovery Store Outside The Cluster" in [spec/decisions/deferred.md](../deferred.md) is reopened and this outcome adopted; they add to the core and never replace it. Requirements trace to [overview section 14](../../overview.md) and
> [overview section 19](../../overview.md).
>
> The capitalized key words are normative, with the meaning the constitution gives them. Each requirement is a single self-contained sentence carrying exactly
> one obligation, under a stable heading. The recovery store and its write interval are declared defaults.

## Purpose And Scope

Under this outcome the durable store does not survive losing every node of a cluster, so a recovery
store outside the cluster holds what a new cluster needs to resume every session, and a new cluster
restores from that store or from backup. This document fixes what the recovery store holds, when it is
written, how a restore orders its records, and the interface that isolates it. The drain, the epoch
fence, and the restore steps that hold under every outcome are in
[core/durability-and-deployment.md](../../core/durability-and-deployment.md).

## The Recovery Store

### A Recovery Store Outside The Cluster Carries Sessions

A recovery store outside the cluster MUST hold what a new cluster needs to resume every session.

The recovery store MUST hold each session's state and cursors, lease generation, timers, subscriptions,
key-value slot, pending inbox events, in-flight dispatch records, and the names of its frozen blobs.

A transcript MUST stay in the blob store rather than in the recovery store.

A recovery-store record MUST be an envelope under the tenant's keys.

The recovery store MUST hold each tenant's configuration record and each tenant's session index beside the session records.

The recovery store MUST hold each topic's subscriber index, delivery cursor, and queue tail position.

A session's final recovery-store record MUST carry its tombstone and any pending-close record.

### Records Are Written At Freeze And Shutdown

A session's recovery-store record MUST be written at each freeze and at shutdown.

A dirty timer change MUST be written to the recovery store within the configured interval.

An orderly shutdown MUST lose nothing.

A crash MUST lose at most what the last freeze did not cover.

A change to a session's durable state MUST reach the recovery store within the configured write interval, whether or not the writer restarted within that interval.

A partition that holds no session MUST cost the recovery-store writer no more than the check that it holds none.

### A Record Larger Than The Item Bound

A recovery-store record larger than the store's item bound MUST be written in parts or by reference to the blob store, with a head that identifies them.

A restore MUST reassemble a record written in parts or by reference.

### State That Is Not Per-Session Is Also Preserved

Tenant-level records written by governance-tier calls other than an emergency control MUST be written through to the recovery store before the call is acknowledged.

A restore MUST apply each tenant's configuration record, its session index, and the tenant-level records that confer or revoke authority before any session record, so that it never loses an acknowledged grant, re-creates a revoked one, or resumes a session whose tenant is unconfigured.

A restore MAY apply tenant-level records that only deduplicate or stage in parallel with the session records.

An operation that depends on a tenant-level record that is not restored MUST be refused as unavailable with a retry delay.

A governance-tier write whose record cannot be made durable within the bounded retries MUST be refused as unavailable and apply nothing.

The dedupe record of a state-changing request other than a publish or a send MUST be written to the tenant-level records before the request is acknowledged.

A session-scoped emergency control MUST be written to the session's recovery-store record before the control is acknowledged.

A definition-scoped or tenant-scoped emergency control MUST be written through to the recovery store's tenant-level records before it is acknowledged.

The write-through of an emergency control MUST be retried a bounded number of times.

When the recovery store is unreachable, the acknowledgement of an emergency control MUST state that the control is in force but not durable across a restore.

The runtime settings record MUST be written through to the recovery store before the change is acknowledged.

### The Recovery Store Is Behind One Interface

The recovery store MUST be accessed through one narrow interface, so that nothing outside that interface depends on it.

The recovery store's interface MUST have a simulator.

The recovery store MUST be protected against deletion by any deployment or operator action other than its planned removal.

The recovery store MUST keep point-in-time recovery for the configured window.

Every resource that exists only for the recovery store MUST be marked as such, so that removing the recovery store removes all of them.

Writes to the recovery store MAY be disabled by configuration on a stage that holds test data only.

## Restore

### The Restore Source Is The Recovery Store Or A Backup

A new cluster MUST restore every session from the recovery store or from backup.

### A Loss Of The Store's Contents Under Running Nodes

The nodes MUST detect that the durable store has lost its contents while they run.

Every node MUST withdraw readiness when a loss of the store's contents is detected.

A node MUST restore readiness only after the restore from the recovery store completes.

A loss of the store's contents, the restore's progress, and the resumption MUST each be published as an event.

The count of losses of the store's contents MUST raise an alarm.

A session that was in the middle of a step at a loss MUST retry what was in flight after the resumption.
