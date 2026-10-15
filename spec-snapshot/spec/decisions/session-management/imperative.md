# Outcome: Imperative Session Management

> **OUTCOME SPECIFICATION.** These requirements apply when the decision record ([spec/record.md](../../record.md) part A) keeps the fixed default imperative for who owns the list of root sessions; they add to the core and never replace it. This document defines how a system of the tenant's creates a root session with its configuration through the frontend, reconfigures it, ends it, registers a keyed template, and receives its lifecycle events. Requirements trace to [overview section 10](../../overview.md) and [overview section 19](../../overview.md).
>
> The capitalized key words are normative, with the meaning the constitution gives them. Each requirement is a single self-contained sentence carrying exactly one obligation, under a stable heading. The cap on the number of root sessions a tenant owns is a declared default.

## Purpose And Scope

Under this outcome a system of the tenant's owns the list of root sessions. It creates each root session with its configuration through the frontend, reconfigures it, ends it, and reacts to the lifecycle events the platform delivers to it. The platform runs each root session it is asked to run and reports what happens to it. A root session's configuration has the shape the glossary gives a definition, and the configuration schema it is validated against is that shape. The invariants that hold under every outcome, among them session origins and liveness by mechanism, are in [core/sessions-and-partitions.md](../../core/sessions-and-partitions.md) and [core/the-frontend.md](../../core/the-frontend.md). A sub-session, a fork, and a keyed instance are owned by their parent or their template under every outcome and are not the subject of this document.

## Creating And Ending Root Sessions

### The API Accepts Creation With Configuration

The API MUST accept creation of a root session together with its configuration.

A root session created through the API MUST be owned by the caller's principal.

A root session created through the API MUST record a caller as its origin.

### The API Accepts Ending

The API MUST accept ending a root session.

### Reconfiguration Applies At A Turn Boundary

A reconfigure control operation MUST replace a root session's configuration.

## Keyed Templates

### A Template Is A Configuration Plus A Key Scheme

The API MUST accept registration of a keyed template.

A keyed template MUST consist of a configuration and a key scheme.

A registered template MUST be owned by the principal that registered it.

## Policies

### A Policy Holds Settings A Session Inherits

The API MUST accept a policy record, referenced by name, that holds settings a root session may inherit.

A root session's configuration MAY list policies, whose settings apply in list order with the session's own fields last.

A change to a policy MUST apply to each session that lists it at that session's next turn boundary.

## Control Operations And Grants

### Each Operation Passes The Control-Operation Hook

Creating a root session, ending it, reconfiguring it, and registering a keyed template MUST each be a control operation.

### A Caller Holds A Grant

A caller MUST hold a grant covering the operation to create, end, or reconfigure a root session or to register a keyed template.

## Ownership And Lifecycle

### The Owner Decides Whether A Root Session Exists

The owner of a root session MUST be the party responsible for deciding whether it should exist.

The set of root sessions MUST be reconciled by the owning system rather than by a person.

### Lifecycle Events Reach The Owner

The platform MUST deliver a root session's lifecycle events to its owner through event hooks.

A root session's configuration MUST identify the event hook destinations that receive its lifecycle events.

## Caps And Validation

### A Refusal Over A Cap States The Cap

The number of root sessions a tenant owns MUST be bounded by a configured cap.

A request refused because it would exceed the root-session cap MUST carry a status that states the cap.

### Configuration Is Validated And Governed

A root session's configuration MUST be validated against the configuration schema and the tenant's upper bounds at creation and at reconfiguration.

A root session's configuration MUST pass the input deciders.

A configuration that fails validation MUST be refused with a reason.

A refused reconfiguration MUST leave the previous configuration in place.

A configuration change that widens authority MUST defer to the approver the tenant's governance identifies, unless the caller's grant covers that class of change.
