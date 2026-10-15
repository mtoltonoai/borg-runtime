# Outcome: The Specification Product

> **OUTCOME SPECIFICATION.** These requirements apply only when the deferred entry "The Specification As A Product" in [spec/decisions/deferred.md](../deferred.md) is reopened and this outcome adopted; they add to the core and never replace it. This document defines how a requirement becomes an entity that code and tests cite one by one, how a specification version moves from Draft to Green against the code, and how the change gate uses the same data as a check that can block a change but never admits one alone. Requirements realize [Core Principle VII](../../../constitution.md) and [Protected Guarantee "Agents Never Approve"](../../../constitution.md) and trace to [overview section 15](../../overview.md) and [overview section 19](../../overview.md).
>
> The capitalized key words are normative, with the meaning the constitution gives them. Each requirement is a single self-contained sentence carrying exactly one obligation, under a stable heading. The requirement-gate tool and the export format are declared defaults; the evidence a gate reads is pinned by the [evidence-record contract](../../contracts/evidence-record.md).

## Purpose And Scope

Whether code still matches its specification should be computed rather than assessed by a person. This outcome fixes requirements as citable entities, the specification lifecycle, and the gate's role as a check that blocks but never admits. The mechanism that runs the gate at a hook is the governance core; whether authority widens with the gate's record is the fixed default for how authority is widened, checkers-only, recorded in [spec/record.md](../../record.md) part A, with its alternatives in the entry "Human Approvals And Record-Based Autonomy" of [spec/decisions/deferred.md](../deferred.md).

## Requirements As Entities

### A Requirement Is Cited One By One

A requirement MUST have an identity that code and tests cite individually.

A reworded requirement MUST invalidate exactly the citations of that requirement and no others.

Retitling or moving a document MUST NOT invalidate a citation.

### Documents Are Trees Of Typed Blocks

A specification document MUST be a versioned tree of typed blocks.

A requirement block MUST carry a stable id, a level, a polarity, a statement, conditions, parameters, relations, and rationale.

A requirement's text MUST be generated from its block, so that extraction is exact.

A requirement block's id MUST survive an edit, a move, a split, and a merge.

A requirement block's id MUST resolve to the requirement's identity tuple at each pinned version.

A requirement block's id MUST NOT be reused.

### An Export Is The Pin

Each repository MUST commit an exported specification that citations resolve against.

A citation MUST identify a requirement by its id, compared against the statement at the pinned version.

## The Lifecycle

### A Version Moves From Draft To Green

A specification version MUST move through Draft, Preview, Target, and Green.

Preview MUST show the failures a version would add and the failures it would resolve against the code.

Target MUST open one tracking ticket whose work list is the gate's failures.

Green MUST be reached exactly when the trusted reports on every bound repository's main line show no failures against the version.

The tracking ticket MUST close when the version reaches Green.

Whether a version goes Green MUST be decided by a mechanism rather than by a person.

A version whose requirement relations are unresolved, cyclic, or make one requirement both required and forbidden at the strongest level MUST be blocked at Preview.

### Publishing Is Governed

Publishing a version MUST be a governed operation.

A narrowing or neutral version MAY publish by default.

A version with a widening change to a mandatory requirement MUST defer to the approver the tenant's governance identifies.

A version that weakens a requirement MUST NOT be approved by its author.

## The Change Gate

### The Gate Blocks But Never Admits

The gate MUST compare a change's failures against the base commit's failures.

The gate MUST run with the base commit's configuration.

The gate MUST be able to block a change that adds a failure.

The gate MUST NOT admit a change on the traceability check alone.

### A Requirement A Change Modifies Needs Execution And Mutation Testing Or A Proof

A mandatory requirement a change modifies MUST be discharged by execution plus mutation testing or by a proof.

An exception on a mandatory requirement MUST be treated as a widening change.

### Evidence Is The Checker's, Not The Agent's

The gate MUST decide on an evidence record from a trusted runner rather than on an agent's account.

### The Gate Runs In Shadow First

The gate MUST run in shadow, recording what it would decide, before it blocks.

The gate MUST begin blocking only once its shadow record meets the threshold its gate spec states.

## Delegation

### Gate Checking Is Delegated, Not Native

The requirement-gate tool MUST run as a service outside the platform, behind a tool server or, where the built collaboration platform of the deferred register is adopted, as a component of the collaboration platform, never as platform state.

The agent platform MUST contain no code or configuration specific to the requirement-gate tool.

### An Implementer Cannot Mark Its Own Work Done

A failure item MUST close only from a trusted report, never from a tool an implementer holds.

A tool that writes a specification MUST declare a spec-write call type, so that the hook can deny it to an implementer.
