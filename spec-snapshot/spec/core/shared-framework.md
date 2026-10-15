# Core: The Shared Framework

> **CORE SPECIFICATION.** Behavior and invariants that hold under every outcome of every decision, free of implementation detail. This document defines the generic components that the platform and any product built alongside it, such as a collaboration platform or a workspace service, build on, and the discipline that a mechanism is extracted into the framework from working cases rather than designed ahead of them. Requirements realize [Core Principle VI](../../constitution.md) and [Core Principle I](../../constitution.md) and trace to [overview section 16](../overview.md).
>
> The capitalized key words are normative, with the meaning the constitution gives them. Each requirement is a single self-contained sentence carrying exactly one obligation, under a stable heading. Which store, transport, and object store realize these components are declared defaults.

## Purpose And Scope

A generic mechanism belongs in one place that every product shares and that improves with each of them.
This document fixes the discipline of what goes into the framework and the behavioral obligations of
the components the products depend on. The detailed behavior of a component that one core document uses
is stated in that document; this document states what makes it a shared component and what it must
guarantee to more than one user. The discipline itself is stated in constitution.md: a mechanism that more than one product needs is a component of the shared framework, extracted from at least two working cases, and no product keeps a second copy of one the framework provides (Core Principles I and VI); every component runs under the deterministic simulation framework with a simulator for each external dependency, verified by a contract test against the real dependency (Core Principle IV).

## The Components

### The Partitioned Actor Set

The framework MUST provide a partitioned actor set with one coordinator per set.

The coordinator MUST assign partitions to nodes by a pluggable placement policy.

The coordinator's assignment from one consistent view of membership and the durability of the assignment table are specified in spec/core/sessions-and-partitions.md.

A partition's ownership MUST be fenced by generation.

The actor set MUST support draining a node's partitions.

The coordinator MUST stay off the critical path.

### Reads At The Woken Version

The framework MUST provide a read pinned at the version a watch woke for, so that the reader always sees
the append that woke it.

### Deadlines On The Environment

The framework MUST provide a sleep to an absolute instant.

The framework MUST provide a deadline queue with insert, cancel, and next-deadline.

The per-node wall clock under simulation, whose offsets, drift, and steps can be injected, is specified in spec/core/simulation-and-conformance.md.

### Envelope Encryption

The framework MUST provide one envelope-encryption implementation, generic over the resource and its authenticated data.

The envelope-encryption component MUST realize the key hierarchy the stored-form contract pins.

The envelope-encryption component MUST provide a deterministic mode for immutable blobs.

The envelope-encryption component MUST support a root dedicated to each tenant, whether the tenant's own or one the service holds for it.

The re-wrap of a long-lived session's data key per epoch without a call to the key service is specified in spec/core/encryption-and-blobs.md.

The envelope-encryption component MUST provide a key-service client built for the key service's limits.

The envelope-encryption component MUST provide a key per owner whose destruction makes that owner's data unreadable.

The envelope-encryption component MUST release exactly one owner's data key to an authorized off-host caller without exposing another's.

A plaintext data key MUST NOT be stored at rest.

### The Blob Store

The framework MUST provide the owner-scoped blob store, in which a blob is addressed by the hash of its ciphertext.

That a blob is never deduplicated across tenants, and that owners and pins are recorded from the first write, are specified in spec/core/encryption-and-blobs.md.

### The Change Feed

The framework MUST provide a change feed as an ordered log.

A change feed's cursor MUST be resumable and fenced by a lease generation.

An external effect a change-feed consumer makes MUST carry an id derived from its position.

The framework MUST provide a resumable form of the change feed over the network.

A source MUST serve changes in a prefix-closed order.

A source MUST NOT answer with a head below a position it has served.

A position the source cannot serve MUST be answered with a distinct gone status rather than silently resumed.

A change feed's positions MUST be qualified by the restore epoch.

A cursor MUST commit in the consumer's own transaction, checked against the consumer's generation inside it.

A handler MUST produce the same effects in the same order on every redo of a change.

A change a handler permanently rejects MUST go to a dead-letter record with an alarm.

The cursor MUST move past a change that went to the dead-letter record.

The network form of a feed MUST take the tenant from the authenticated caller and never from the path.

### The Live-Stream Hub

The framework MUST provide a live-stream hub fed by a change feed.

The hub MUST apply per-viewer filters and access checks.

The hub MUST let a viewer resume from a last-seen id.

The hub MUST send a reset to a viewer whose last-seen id is older than the bounded retention.

The hub MUST hold a bounded buffer that drops a slow viewer.

A viewer's access MUST be decided afresh at every open and resume.

A viewer's access MUST be decided again when a change marked as an access change arrives.

A node MUST bound the buffers of all its streams together.

An ungated frame MUST NOT be replayed or stored.

### Rate Limiting

The framework MUST provide keyed rate limiting by tenant, principal, and route class, and by the attested person where a service acts for people.

Rate limiting MUST answer a throttled caller with a retry-after.

Rate limiting MUST stay bounded under a large number of distinct keys.

The attested person a limit keys on MUST come from the verified chain rather than from a request field.

A limiter charge MUST happen before the call guards admit, so that a throttled request claims no receipt and moves no fence.

A request whose wait would exceed its class's bound MUST be refused at once with the exact delay.

A burst of new keys from one tenant MUST NOT shut out another tenant's new callers.

A route that must never wait or be refused MUST carry an exempt mark the product sets, never the caller.

### Caller Authentication And Call Guards

The framework MUST provide caller authentication that turns a request into a caller whose identity comes from authentication.

The framework MUST provide call guards that dedupe by call id.

The call guards MUST fence by lease generation.

A receipt's commit in the same transaction as the call's effect is pinned by spec/contracts/tool-call.md.

### Node Transport Is Authenticated

Every connection between the platform's nodes, including the durable store's replication, membership, and control traffic and the coordinator's placement and partition-transfer traffic, MUST authenticate both peers.

The credential from which peer authentication is derived MUST be scoped to one cluster.

The credential from which peer authentication is derived MUST be rotated within the configured interval.

### Durable Store Durability

The framework MUST provide the durable store with the durable snapshot, the durable index, and the log that the adopted durability outcome requires.

The durable store MUST survive a full-cluster restart.

The durable store MUST have a defined backup and restore.

### Self-Describing Ids

A parser MUST accept only an id's canonical form and refuse every other spelling.

Codes MUST come from one append-only registry in which a code never changes and is never reused.

A decoder MUST report an unregistered code as unregistered rather than guess.

## Evolution

### Components Change Through Their Own Review

A change to a framework component MUST go through the framework's own review.

A component MUST be consumed by a product only through the framework's released version.
