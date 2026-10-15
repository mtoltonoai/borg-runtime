# Outcome: Both Paths

> **OUTCOME SPECIFICATION.** These requirements apply only when the deferred entry "Declarative Root Sessions And A Reference Definition Source" in [spec/decisions/deferred.md](../deferred.md) is reopened and the both outcome adopted; they add to the core and never replace it. This document states how the imperative path in [imperative.md](./imperative.md) and the declarative path in [declarative.md](./declarative.md) coexist when both are adopted. Requirements trace to [overview section 10](../../overview.md) and [overview section 19](../../overview.md).
>
> The capitalized key words are normative, with the meaning the constitution gives them. Each requirement is a single self-contained sentence carrying exactly one obligation, under a stable heading.

## Purpose And Scope

Under this outcome standing agents are owned by the definitions a tenant's sources serve, and root sessions started for one task are created by callers through the frontend. Two paths to a root session exist, so the requirements below fix which path owns each root session, how a root session on each path ends, and that both paths are governed by the same hooks and create keyed instances by the same mechanism.

## Ownership

### Reconciliation Covers Only Definition-Owned Sessions

A caller-owned root session MUST be excluded from reconciliation against the desired set.

A caller-owned root session MUST NOT be reconfigured by a controller.

## Ending Follows The Owner

### A Definition-Owned Session Follows Its Source

A definition-owned root session MUST NOT be ended through the API except by the emergency controls.

A definition-owned root session MUST NOT be reconfigured through the API.

### A Caller-Owned Session Ends By Its Owner

A caller-owned root session MUST end only by its owner's instruction, by stop admission, or by the emergency controls.

## One Mechanism For Both Paths

### Both Paths Pass The Same Hooks

The imperative path and the declarative path MUST pass the same hooks with the same deciders.

### Keyed Instancing Is One Mechanism

A keyed template registered by a caller and a keyed definition served by a source MUST be instantiated by the same mechanism.
