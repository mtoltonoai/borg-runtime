# Constitution

> **What this document is.** The fixed invariants of the platform and the products built beside it, stated as normative requirements. Every other specification, the contracts under
> `spec/contracts/`, the core specifications under `spec/core/`, and the outcome specifications under
> `spec/decisions/`, inherits these and must not contradict them. `spec/decisions/` holds the decisions, a register of deferred decisions, and
> their outcomes: one document per decision, and one directory per decision or fixed default holding the
> requirements that apply only under each outcome. The architecture these invariants serve is described in
> [spec/overview.md](./spec/overview.md); the vocabulary they use is defined in
> [spec/glossary.md](./spec/glossary.md). The general tenets and the platform tenets P1 to P7 are stated
> as the Core Principles and the Platform Principles of this document. How an implementer builds from this
> specification is described in [spec/README.md](./spec/README.md).
>
> The key words **MUST** and **MUST NOT** state an absolute requirement; **SHOULD** and **SHOULD NOT** state a requirement that an implementation may set aside only with a recorded reason; **MAY** states an optional behavior. Every other specification uses these key words with this meaning. Each requirement below is a single self-contained sentence under a stable
> heading, so it can be extracted and cited exactly. A requirement's identity is the tuple (this file,
> its section, its quoted sentence); there are no separate identifiers, and changing a sentence's wording
> flags every citation that no longer matches it. A requirement mentions no concrete engine, store,
> provider, library, or numeric default; those are recorded in [spec/record.md](./spec/record.md).

## Core Principles

### I. Minimal And Concrete

A mechanism of the platform MUST be a concrete primitive with a defined interface that a program can be
written against, rather than a universal abstraction other mechanisms are encoded into.

A generalization of a mechanism MUST be extracted from at least two working cases that use it, rather
than designed ahead of them.

### II. Event-Driven, Never Polling

A component MUST NOT poll a dependency for a change that the dependency can notify it of.

A session MUST be woken by an event, a timer, or a deadline addressed to it, rather than by a periodic check for work.

### III. Everything Is Observable

Every state change the platform makes MUST be published as a specific event.

A metric MUST be split only by dimensions drawn from closed sets.

An identifier of an open set, such as a tenant, a session, a principal, or a tool name, MUST be carried
on events as a field rather than as a metric dimension.

### IV. Deterministic By Construction

Every component MUST run under the deterministic simulation framework with every external dependency
replaced by a simulator.

Every external dependency MUST have a simulator that injects the failures the real dependency exhibits.

A simulator MUST be verified by a contract test that runs the same expectations against the simulator and against the real dependency.

A component MUST take its entropy from the simulation's entropy source rather than from an ambient
source of randomness.

A component MUST NOT iterate a collection in an order that varies between runs of the same inputs.

### V. Secure By Default

Every durable record of a tenant's content MUST be encrypted at rest.

A component that cannot obtain a policy decision, a key, or a credential MUST fail closed.

A key MUST be chosen only by a trusted input, never by a value read from a store or a payload.

### VI. Generic Mechanisms Belong In The Shared Framework

A mechanism that more than one product needs MUST be built as a component of the shared framework
rather than kept inside one product.

A product MUST NOT keep a second copy of a mechanism the shared framework provides.

### VII. What Repeats Becomes A Mechanism

Whether a change is correct MUST be decided by a mechanism rather than by an approval.

A decision MUST reach a person only when no mechanism can decide it.

A decision that reached a person MUST be recorded as a candidate for conversion into a mechanism.

## Platform Principles

### P1. The Core Executes And Decides No Policy That Configuration Can Supply

The core MUST execute sessions, deliver events, call models, run tools, and persist state without
deciding any policy a tenant could supply as configuration.

### P2. Behavior Is Configuration

Policies, prompts, tools, models, deciders, thresholds, and triggers MUST be data the core reads rather
than code the core carries.

The core MAY carry safe defaults for configuration a tenant has not supplied.

### P3. Change Without Redeploying

A behavior change MUST take effect in a running session at its next turn boundary without a redeploy.

A code change MUST be required only for a new mechanism, such as a new executor kind or a new hook point.

### P4. Generic Over The Domain

The core MUST NOT interpret a topic, a payload, a tool, or a call type.

Meaning MUST be given to a topic, a payload, a tool, or a call type by an adapter rather than by the
core.

### P5. Durable State, Disposable Memory

Every part of a session's state MUST be kept in the durable store.

Everything a node holds in memory MUST be a cache that can be dropped and rebuilt from the durable
store at any moment.

### P6. One Handling Per Failure, Every Side Effect Redoable

Every failure class MUST have exactly one defined handling and one defined state the session ends in.

Every side effect MUST carry an idempotency key so that redoing it is safe.

### P7. The System Improves Itself Through Its Own Primitives

Every output the platform produces about its own behavior, such as a usage record, an observation record,
or a decision episode, MUST be data that can feed back as configuration through the same change control
as any other configuration.

## Boundaries Of The Platform

### The Platform Hosts No Programs For Its Tenants

The platform MUST NOT execute tenant-supplied code other than a decider running in the sandbox.

A tool MUST be a service the platform calls rather than code the platform hosts.

An agent definition MUST be data the tenant supplies rather than code written into the platform.

### Irreversible Choices Are Decided First

A property that cannot be changed once data has been written under it MUST be decided before the first
durable record is written.

A property that can be added later without rewriting existing data MAY be deferred until use demands it.

## Protected Guarantees

These are the minimum guarantees that no evolution policy may weaken. They exist because the discipline that governs how these specifications change is itself amendable; these guarantees bound that self-amendment.

### The Tenant Is The Boundary

Every durable record, principal, grant, subscription, and event MUST carry its tenant.

Content derived from one tenant's data MUST NOT reach another tenant.

Every tenant's keys MUST derive from a root key dedicated to that tenant, whether the tenant's own or one the service holds for it.

Destroying a tenant's root key MUST make the tenant's data unreadable everywhere, backups included.

Data MUST NOT be written under a root key shared between tenants.

### Identity Comes From Authentication

The acting identity of a call MUST come from authentication rather than from an argument, a header the
caller controls, or a payload.

Delegated authority MUST only narrow from grantor to grantee.

A human sign-on credential MUST NOT be used to run automation.

### Secrets Never Reach The Model Or The Records

A secret MUST NOT appear in model context, a transcript, session state, an event, an observation record,
a metric, or a log.

### Agents Never Approve

An agent MUST NOT hold a capability to approve a change.

Authority MUST be widened only by a sound checker or by a person the tenant's governance designates.

### Security Guarantees Are Never Downgradable

The fail-closed guarantee MUST NOT be reducible to a warning by any configuration.

The tenant boundary MUST NOT be reducible to a warning by any configuration.

### Contracts Change Only By Coordinated Act

A change to a contract that is not additive with respect to deployed clients MUST carry a version
increment.

A change to a contract that is not additive with respect to deployed clients MUST carry a stated
migration path.

### Scopes Are Recorded From The First Event

Every event MUST carry the access scope its source allowed, from the first event the platform accepts.

### Amendment Discipline

An amendment to this constitution MUST be recorded with its rationale.

An amendment that weakens a protected guarantee MUST require explicit human approval.

## Governance

This constitution supersedes all other specifications where they conflict on an invariant. The contracts
under `spec/contracts/` pin the interfaces that independently deployed parties honor across versions; the
core specifications under `spec/core/` describe the behavior every valid solution has, and the outcome
specifications under `spec/decisions/` describe the behavior that applies only under an adopted outcome;
both must satisfy these invariants. Compliance is checked by the requirement gate described in
[spec/core/simulation-and-conformance.md](./spec/core/simulation-and-conformance.md),
under which every gating requirement here carries an implementation citation and a test citation, while a governing requirement binds to the change-process check, and by the
simulation scenarios that exercise the invariants. Amendments follow the Amendment Discipline above and
are traced against the architecture in [spec/traceability.md](./spec/traceability.md).

**Version**: 0.2.0 | **Ratified**: 2026-10-08 | **Last Amended**: 2026-10-09
