# Core: Tenancy And Confidentiality

> **CORE SPECIFICATION.** Behavior and invariants that hold under every outcome of every decision, free
> of implementation detail. This document defines the tenant as the trust and data boundary, the
> properties the substrate fixes from the first record because they cannot be changed once data has been
> written under them, and the access scopes that keep a reader
> inside a tenant seeing only what its sources allowed. Requirements realize
> [Protected Guarantee "The Tenant Is The Boundary"](../../constitution.md) and
> [Protected Guarantee "Scopes Are Recorded From The First Event"](../../constitution.md) and trace to
> [overview section 8](../overview.md).
>
> The capitalized key words are normative, with the meaning the constitution gives them. Each requirement is a single self-contained sentence carrying exactly
> one obligation, under a stable heading.

## Purpose And Scope

A tenant is the set of all principals who could ever be permitted to read any of its data. This document fixes what identifies a
tenant, what never crosses one, and how confidentiality works inside one through access scopes. The
cryptographic root of the boundary is in spec/core/encryption-and-blobs.md. How many tenants the platform
serves, and when, is settled by decision [tenancy-scope](../decisions/tenancy-scope.md); the requirements
under each outcome are in spec/decisions/tenancy-scope/.

## The Tenant Is The Boundary

Every durable record, principal, grant, subscription, and event carries its tenant, and a tenant is never a metric dimension; constitution.md states both, under the protected guarantee "The Tenant Is The Boundary" and in Core Principle III.

### Authority Does Not Extend Beyond The Tenant

A grant MUST cover only its own tenant's sessions and resources.

A topic and an observation queue MUST be scoped to one tenant.

The platform's own operators MUST reach a tenant's sessions through grants, as every other principal does.

A session's send MUST address its target only through a form the platform computes with the sender's own tenant: a template with its key, the session's parent, or a sub-session the session started.

A partition MUST refuse an inbox entry whose recorded publisher tenant differs from the receiving session's tenant.

A partition MUST publish the refusal as an audit event.

A node's credentials for the object store and the key service MUST be scoped to the tenants the node hosts, so that a compromised node reaches only those tenants.

A store client MUST refuse an operation whose tenant does not match the credentials it was handed.

### Nothing Derived Crosses

A content cache MUST be keyed by tenant.

Labels, episodes, and trained models MUST stay within their tenant.

Content shared between tenants MUST be published on purpose rather than reach the other tenant through a derived store.

### Ids Are Portable

A durable id MUST be unique across tenants.

A durable id MUST NOT encode where it runs.

A durable id MUST NOT be reissued to a different entity, across a restore included.

A durable id MUST carry the kind of entity it identifies and the version of its format.

A durable id MUST have exactly one canonical text form.

### A Lookup Reveals Nothing Across Tenants

A uniqueness violation on an index that spans tenants MUST be an internal error rather than a refusal the caller sees.

## Irreversible Properties

The key properties, that each tenant's keys derive from a root key dedicated to it, that destroying that root key makes the tenant's data unreadable everywhere including backups, and that no data is written under a root key shared between tenants, are stated in constitution.md under the protected guarantee "The Tenant Is The Boundary"; their mechanism is in spec/core/encryption-and-blobs.md. The one usage record per model attempt, tool call, and decider call, carrying its tenant and principal chain, is pinned by spec/contracts/usage-record.md.

### Root Key Administration Is Separated

The identity that deploys the platform's infrastructure MUST NOT be able to use or administer a tenant's root key.

A tenant's root key MUST grant nothing to a principal that its own policy does not identify.

A role that administers a key MUST NOT be able to use it.

Any attempt to disable a tenant's root key or to schedule its destruction MUST raise an alarm.

Destruction of a root key MUST take effect only after a configured waiting window.

### Retention Is Per Tenant

Retention MUST be tenant configuration, per kind of data.

Audit metadata MUST be retained longer than content.

Deletion by key destruction is specified in spec/core/encryption-and-blobs.md, and the simulation scenario that runs two tenants and checks that nothing crosses between them is in spec/core/simulation-and-conformance.md.

A tenant MAY configure an idle duration after which a session ends.

Copies of episode content MUST be a retention kind of their own in the tenant's configuration.

## Confidentiality Inside A Tenant

Every event carries the access scope its source allowed, from the first event the platform accepts, because a scope cannot be recovered once content is compacted or distilled; constitution.md states it under the protected guarantee "Scopes Are Recorded From The First Event", and spec/contracts/event-envelope.md pins the field.

### Scopes Propagate As Unions

A transcript range, a compaction, and an observation record MUST carry the union of its inputs' access
scopes.

### A Reader Passes A Scope Check

A consumer or derived store MUST pass a read check against the access scope before it reads.

Delivery to a viewer MUST enforce the read check.
