# Core: Encryption And The Blob Store

> **CORE SPECIFICATION.** Behavior and invariants that hold under every outcome of every decision, free of implementation detail. This document defines encryption of sessions at rest, keeping the key service off the turn's critical path, and the owner-scoped blob store that holds frozen transcripts and large outputs. Requirements realize [Core Principle V](../../constitution.md) and [Protected Guarantee "The Tenant Is The Boundary"](../../constitution.md) and trace to [overview section 9](../overview.md).
>
> The capitalized key words are normative, with the meaning the constitution gives them. Each requirement is a single self-contained sentence carrying exactly one obligation, under a stable heading. The stored form is pinned by the [stored-form contract](../contracts/stored-form.md); the cipher modes, the hash, and the cache lifetime are declared defaults.

## Purpose And Scope

Encryption enforces the tenant boundary cryptographically and makes deletion a key-destruction operation. The blob store
holds everything too large for the durable log. This document fixes the behavior around the stored
form: how keys resolve without stalling a turn, how a blob is owned and collected, and how deletion
reaches every copy. How a blob is addressed, that its name reveals nothing about its plaintext, that a store or cache verifies it without any key, and that it is immutable once written are pinned by spec/contracts/stored-form.md. Administration of a tenant's root key is reserved to a person, as spec/core/tenancy-and-confidentiality.md and spec/core/identity-delegation-and-secrets.md specify.

## Encryption At Rest

### Sessions Are Encrypted Under An Envelope

Every session's values and blobs MUST be encrypted under the envelope the stored-form contract pins.

A node MUST be able to read a session with at most one call to the key service.

A plaintext key MUST be held only in a per-node cache whose lifetime is the revocation window.

A stage that holds production data MUST store every value and blob under the envelope.

A stage whose stores do not apply the envelope MUST refuse a production tenant at start and on every call.

Content stored without the envelope MUST be discarded with its stage rather than migrated.

A stage whose stores take one key MUST serve exactly one tenant.

A stage MUST hold a tenant's production data only inside an isolation boundary in which the platform's own deployment pipeline is the only principal with administrative authority over the whole boundary.

A stored record that has not been re-sealed MUST stay readable under the format version, the key hierarchy, and the key generation it was sealed with after any of them changes.

A re-seal of a record MUST commit under the record's partition fence, so that the owner's own commits cannot interleave with it.

A plaintext value found where a sealed value is due MUST be treated as a corrupt value rather than read.

Ending or deleting a session MUST make its unapplied inbox entries unreadable.

### The Key Comes From A Trusted List

The root key for a session MUST be identified in the session's configuration by a label.

The label MUST be checked against the node's trusted list of permitted keys.

That a value read from a store never determines which key is used is Core Principle V in constitution.md.

### The Key Service Stays Off The Critical Path

A session's keys MUST resolve when its doorbell wakes it, before a turn needs them.

The decrypt cache MUST refresh a key ahead of its expiry, so that a revocation still takes effect within the window.

A key-service call that must be waited on MUST be hedged and bounded by a short deadline.

Resumes after a failover MUST be paced, so that a burst of session resumes does not exceed the key service's
request quota.

### A Long-Lived Session Re-Wraps Per Epoch

A session that outlives an epoch MUST re-wrap its data key under the new epoch's branch key without a
call to the key service.

## The Blob Store

### A Blob Is Owned And Immutable

Every blob MUST have one owner and be stored under its tenant and owner.

The owner's writer MUST be the only writer of a blob's pins, transfers, and key deletion, under the owner's generation, so that a new reference cannot race a delete.

An owner id MUST NOT be reused.

### Sharing Is An Explicit Pin

Sharing a blob across owners MUST be an explicit pin with a holder and an expiry, recorded through the
owner before the reference is used.

Ownership MUST transfer only through the current owner.

A blob MUST NOT be deduplicated across tenants.

Deduplication inside a tenant, where it is shown to reduce cost, MUST be a tenant-owned class addressed by a keyed
hash under a tenant key.

A pin MUST record its scope, its holder class, and its own expiry, written by the owner's writer in the commit that gives out the reference.

A pin for a deleted owner MUST be refused.

A pin MUST NOT outlive its owner's key.

A pin's name MUST NOT be reused.

The holder a read is checked as, and the pin it reads through, MUST be set by the platform from its own records, never from a payload, a decider input, a tool argument, or a program a tenant supplies.

A pin MUST NOT admit a holder of another tenant.

A read through a lapsed or missing pin MUST fail closed with the same not-found answer whether or not the blob exists.

### An Attachment Is Copied Under Its Receiver

An attachment a session receives MUST be copied under the receiving session's own owner before the event applies.

### A Permission Refusal Is Never Not Found

A permission refusal from the object store MUST be an internal error with an alarm, never a not-found answer.

### Reclamation Never Removes A Referenced Blob

The blob store MUST NOT reclaim a blob that any owner or pin references.

Owners and pins MUST be recorded from the first write.

Reclaiming bytes MUST run after the write, at low priority, rather than as part of the write.

Reclamation of unreferenced blobs is a deferred decision, recorded in spec/decisions/deferred.md with the invariant that keeps it possible.

### Deletion Is Key Destruction

Deletion MUST come from destroying keys.

Destroying a key MUST take effect in the blob store, caches, and backups.

The node cache MUST hold only ciphertext, so that destroying a key revokes cached copies within the
decrypt cache's window.

A copy of key material MUST NOT be kept in a store whose recovery window would leave it readable after its key is destroyed.

A session's data key MUST be wrapped only under a key whose every stored copy is destroyed with the session's owner, never under the tenant's branch key alone.

Destroying a key MUST be followed by the destruction of every plaintext copy a host holds, within the configured bound.

Deleting a session MUST destroy, in the same deletion, the keys of every copy of its transcript ranges made for episodes.

A read of a range that is no longer readable MUST answer not available rather than partial content.

A transfer of a blob to another owner MUST re-seal it under the new owner's key.

### A Content Id Is The Hash Its Consumers Can Compute

A content id MUST be the hash of the bytes its consumers can compute and verify.

A content kind whose bytes a party outside the platform computes MUST be identified by the digest of its canonical plaintext bytes.

A content id MUST resolve only within its tenant unless it is content published across tenants on purpose.

A plaintext content id MUST appear only in records kept under the tenant's key.

An event, usage record, metric, log line, or error text MUST identify a catalog entry by its catalog id and version rather than by its artifact's content id.

A blob's name and its content id MUST be kept as separate values, neither derived from the other.

## Platform-Shared Content

### Platform-Published Content Is Copied Per Tenant

Platform-published content such as decider modules, policy engines, and model weights MUST be copied into each tenant under that tenant's own key rather than shared as stored content across tenants.

A tenant's own writer MUST copy platform-published content from a release location that holds no tenant data.

A tenant's copy of platform-published content MUST be verified against the content ids fixed in the platform's reviewed build.

A tenant MUST be served from a shared compiled form only while the tenant's own stored copy exists and verifies to the same id.

A node MAY share one compiled module or one loaded model per content id in memory across the tenants it hosts, provided the object is built only from platform-published bytes, is immutable, keeps request state per tenant, and serves no mixed batch.

A tenant-authored module MUST NOT share a compiled form across tenants.
