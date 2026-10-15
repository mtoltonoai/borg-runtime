# Contract: The Definition Source

> **CONTRACT.** This document pins the interface through which a tenant's service serves agent
> definitions to the platform, and the shape of a definition itself. It is the interface a tenant's
> source implements as a server and the platform implements as a client, and it is honored by
> both across releases, since the two are released through separate pipelines. Its requirements realize
> [Boundary "The Platform Hosts No Programs For Its Tenants"](../../constitution.md) and trace to
> [overview section 10](../overview.md). This contract is in force only when the deferred entry "Declarative Root Sessions And A Reference Definition Source" in [spec/decisions/deferred.md](../decisions/deferred.md) is reopened and the declarative or the both outcome adopted.
>
> The capitalized key words are normative, with the meaning the constitution gives them. Each requirement is a single self-contained sentence carrying exactly one obligation, under a stable
> heading. The canonical serialization, the digest function, and the long-poll bound are declared
> defaults.

## Purpose And Scope

A definition is the desired configuration of one agent identity, and a tenant serves its definitions
from its own sources under its own change control. This contract fixes what a source serves
(an index, immutable versions, and a change feed), what a definition contains, and what it never
contains. How the platform validates, governs, and adopts a version is specified in the requirements of the declarative outcome, spec/decisions/session-management/declarative.md, which the both outcome includes.

## The Interface

### A Source Is Bound To One Tenant

A source MUST serve definitions for exactly one tenant per binding.

The tenant of a definition MUST come from the binding rather than from the served payload.

A source MUST serve this contract over its well-known interface rather than over the tool protocol.

### The Index Is A Consistent Snapshot At A Rising Revision

A source MUST serve an index that is a consistent snapshot of every definition it serves, at one
revision.

A source MUST NOT serve a revision lower than one it has served before.

An index MUST carry a token a client can present to ask only whether the revision has changed.

A source MUST support a long-poll on the index that returns when the revision changes or when the
configured bound elapses.

An index entry MUST identify the definition and carry its version number, the digest of its current version, its lifecycle intent, its instancing, and the rollout fraction for its current version.

An index entry for a paused singleton MAY carry a handback listing the event ids and dropped ranges the source has taken back since the pause, cumulative since the pause.

The lifecycle intent, the instancing, the rollout fraction, and any handback MUST sit outside the version's digest, so that a change to one of them creates no new version.

A served lifecycle intent MUST apply to its definition whatever became of the version served beside it.

A served rollout fraction MUST apply only to the version whose digest the entry carries.

An entry whose lifecycle, instancing, or rollout value lies outside its closed set MUST be treated as a source fault from which nothing applies.

A definition's instancing MUST NOT change once a version has been accepted for it.

### A Version Is Immutable And Addressed By Digest

A version MUST be addressed by the digest of its canonical serialization.

A source MUST serve the bytes of a version by its digest for as long as any session may pin it.

A version MUST NOT change once it has been served.

A version MUST carry the digest of its parent version, or state that it has none.

A client MUST verify a version's digest by hashing the bytes it received rather than by re-serializing a decoded value.

A client MUST keep an accepted version as the bytes it received.

A definition's version numbers MUST strictly increase.

A version number MUST NOT recur with a different digest, including after the source restores from a backup.

A version body MUST carry its version number, so that one digest identifies exactly one version.

A client MUST treat a known digest served with a version number other than its own as a source fault without fetching it.

### The Change Feed Resumes From Any Revision

A source MUST serve a change feed that resumes from any revision the client specifies.

A change feed asked to resume from a revision the source no longer holds, whether because it restored from a backup or because the revision has passed the feed's retention, MUST answer a distinct gone status, so that the client reads the whole index again.

A source that restores from backup MUST set its revision above the highest it ever served before serving again.

A change-feed entry MUST carry every field an index entry carries.

A change-feed entry for a definition that left the index MUST carry the revision, the definition's identity, and an absent marker.

A change feed MUST support a long-poll that answers as soon as an entry above the given revision exists.

A long-poll that expires with no entry MUST answer with no entries and the feed's current revision.

A client MUST advance its cursor to the revision an empty long-poll answer carries.

### A Source May Announce Its Revision

A source MAY publish its new revision through the platform's event envelope as well as through the feed.

## The Definition

### What A Definition Carries

A definition MUST state its lifecycle intent as one of run, paused, or retired.

A definition MUST state its instancing as singleton, keyed by requester, or keyed by a publisher's key.

A definition MUST carry its prompt components, model and parameters, tool server references, decider
additions, budget caps, deadlines, priority, standing subscriptions, standing schedules, compaction
settings, cache settings, observation settings, and hook destinations by name.

A definition MUST reference a tool server by a name registered with the tenant rather than by an
arbitrary address.

A definition MUST reference a hook destination by a name registered with the tenant rather than by an
arbitrary address.

A definition MUST specify a key only as a label drawn from the tenant's permitted set.

A version body and its components MUST stay within the configured size limits.

### What A Definition Never Carries

A definition MUST NOT carry a principal.

A definition MUST NOT carry a grant.

A definition MUST NOT carry a credential or a secret.

### A Definition Only Restricts

A definition's own deciders and policies MUST only restrict what the tenant's governance allows.

A definition's base priority MUST NOT exceed its binding's upper bound.

### Tools That Write Definitions Declare It

A tool that writes a definition MUST declare the definition-write call type on its spec.

## Lifecycle Semantics

### Only A Served Retirement Ends Sessions

A client MUST treat a definition missing from the index as held rather than retired.

What a source serves MUST end a definition's sessions only when it serves the definition as retired.

### Pause Is Served, Not Inferred

A source MUST serve a paused intent as the only way to pause a definition's sessions.

## Conformance

### One Suite Runs Against Both Sides

A conformance suite for this contract MUST run against a source implementation and against the
platform's client.

The conformance suite MUST check that a source implementation independent of the platform's encoder produces canonical bytes whose digest equals the digest the platform computes for the same value.

## Additive Evolution

### Additive Evolution Of This Contract

A change to this contract MUST be additive with respect to deployed sources and clients, or else carry
an explicit version increment.

A change to this contract that is not additive with respect to deployed sources and clients MUST carry
a stated migration path.

Each field's encoding MUST be declared by this contract rather than chosen per value.

Each integer field of this contract MUST be declared either a number within the declared exact-integer bound, refused above that bound on encode and on decode, or a decimal string under one canonical grammar.

A counter chosen by a party outside the platform MUST be a decimal string.
