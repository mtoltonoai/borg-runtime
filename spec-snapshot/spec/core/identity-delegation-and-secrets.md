# Core: Identity, Delegation, And Secrets

> **CORE SPECIFICATION.** Behavior and invariants that hold under every outcome of every decision, free
> of implementation detail. This document defines the principal on every action, the on-behalf-of chain,
> grants and the narrowing-only rule, per-request authority for shared sessions, the credential broker,
> attribution into external systems, and what the substrate owns versus what higher levels supply.
> Requirements realize [Protected Guarantee "Identity Comes From Authentication"](../../constitution.md),
> [Protected Guarantee "Secrets Never Reach The Model Or The Records"](../../constitution.md), and
> [Core Principle V](../../constitution.md) and trace to [overview section 7](../overview.md).
>
> The capitalized key words are normative, with the meaning the constitution gives them. Each requirement is a single self-contained sentence carrying exactly
> one obligation, under a stable heading. The assertion is pinned by the
> [signed-assertion contract](../contracts/signed-assertion.md); identity providers and token exchanges
> are declared defaults.

## Purpose And Scope

The substrate owns the invariants and higher levels own the policy. This document fixes the invariants:
a principal everywhere, a chain carried with every call, grants as data that only narrow, assertions at boundaries, secrets kept out of the model's context and the platform's records, and accurate attribution. Identity providers, policy content, the broker's backends, and delegation flows are supplied by higher levels.

## A Principal On Every Action

### Every Action Has A Principal

Every action MUST be attributed to a principal.

A session MUST run as a principal.

A session's configuration MUST identify whom it acts on behalf of.

The on-behalf-of chain MUST be carried with every model call, tool call, outbound message, control
operation, and event.

A chain entry MUST carry its relation to the previous entry, delegated or attested, and the party that attested it.

A record that a person runs an agent MUST grant nothing.

A record that a person runs an agent MUST NOT enter an assertion or a chain.

A person identified in a service's request MUST be honored only under a grant that permits that service to attest that person for that action.

A person a service attests MUST enter the chain as an entry attested by that service rather than as the acting principal.

### Principal Ids Carry Their Provider And Tenant

A principal id MUST carry its identity provider, so that adding a provider adds data rather than changing the ids of existing principals.

A principal id carries its tenant, as the constitution's protected guarantee "The Tenant Is The Boundary" requires of every principal.

A person MUST be able to belong to more than one tenant under one provider-qualified id.

A principal id MUST NOT be reissued to another holder.

### A Definition Has Its Own Principal

A definition's principal MUST be derived from its tenant and the definition's identity within the tenant, never from a person.

A definition MUST NOT specify a principal of its own other than the derived one.

A definition MUST carry no grant or credential.

An agent MUST act as its definition's principal rather than as a person or a service, whatever starts its sessions.

### An Agent Identity Outlives Its Sessions

An agent identity MUST be a durable record that outlives any one of its sessions.

Every session MUST carry the agent identity it runs as.

An agent identity MUST be able to have more than one session at a time.

## Enforcement And Fail-Closed

### Enforcement Is At The Fixed Hooks

Identity and authorization MUST be enforced at the control-operation, tool-call, output, and tool-result
hooks.

A hook that cannot obtain an authorization decision MUST fail closed.

## Grants

### A Grant Is Data

A grant MUST record who delegates, to which session or definition, which actions and resources, its
expiry, its conditions, and its tenant.

Checking a grant MUST be a local evaluation rather than a network call.

Deleting a grant MUST revoke it at the next decision.

Every grant MUST be visible to audit and to analysis.

A grant to an automated identity MUST enumerate each allowed action rather than use a wildcard.

### Delegation Only Narrows

A sub-session's grants MUST be references to its parent's rather than copies.

Every decision MUST traverse the chain of grant references.

Revoking an ancestor's grant MUST revoke its descendants'.

A grant from an operator MUST NOT exceed what that operator holds or the role's upper bound.

A sub-session's budget MUST be allocated out of its parent's.

The model-request hook MUST check the shared budget before every call.

### A Shared Session Acts For The Requesting Principal

A decision for a session acting on another principal's event MUST carry the requester's principal and
chain.

The effective authority of a session acting for a requester MUST be the intersection of the session's
grants and the requester's.

A requester MUST NOT obtain, through the session, authority the requester does not hold.

## Assertions

### Services Take Identity From An Assertion

For each call or lease, the platform MUST present a short-lived signed assertion bound to that call, of
the session's principal, chain, tenant, scopes, and lease generation.

A capability-style token MUST be used only at trust boundaries and be short-lived.

A capability-style token MUST NOT be the authoritative record of authority.

A call the platform makes on its own behalf, with no session, MUST carry a call id that can never equal a session's call id, unique per calling actor within the receipt retention.

An audit record of a call the platform makes on its own behalf MUST identify the calling actor beside the call id.

## Secrets

### Secrets Stay Out Of The Model And The Records

That a secret never appears in model context, a transcript, session state, an event, an observation record, a metric, or a log is the constitution's protected guarantee "Secrets Never Reach The Model Or The Records"; this section states the mechanisms. A tool spec identifies a credential it needs by reference, as spec/contracts/tool-call.md pins. A block's reason passes the scrubbing decider, as spec/core/governance.md specifies.

At dispatch the core MUST obtain a short-lived credential from the broker for the principal, chain, and
target.

The core MUST deliver a leased credential only to the executor.

A tool result and an event payload MUST pass the structured-secret decider.

A bearer credential MUST NOT be written to a session's configuration or to the durable store in plaintext.

The full output of a tool call MUST pass the structured-secret check before it is stored.

## The Credential Broker

### The Broker Is Its Own Trust Boundary

The broker MUST decide on the assertion alone, without reading the durable store.

The broker's own base credentials MUST carry no delegated identity of their own.

A credential the broker issues MUST be scoped to the principal, its chain, and the target.

A credential MUST be leased for one call.

A credential MUST NOT be reused for another call.

Every credential issued, and every broker call that fails, MUST be an event that identifies the grant and the
identity requested, never the secret.

A credential exchange MUST be bound to the call's assertion, tenant, session, and call id.

The scopes of an issued credential MUST derive from the call's scopes and nothing broader.

A delegated credential MUST be obtainable only by the broker's own identity.

The broker's identity MUST be assumable only from the service's own deployment.

## Attribution Into External Systems

### No Human Credential Runs Automation

A node's own access to its dependencies MUST come from a machine credential that refreshes without a person.

A connection's lifetime MUST NOT exceed the lifetime of the credential that opened it.

Schema migrations MUST run under an identity the service does not hold at run time.

### The Operator Is Carried Into The Audit Trail

Each operator's sessions MUST act with credentials issued for that operator's delegation, attributable
and revocable per operator.

Where a target system offers an on-behalf-of exchange, the platform MUST present the operator as the
subject and itself as the actor.

Where a target system offers no on-behalf-of path, the platform MUST act as itself and record the
operator in its audit and in the visible text.

The platform MUST NOT present an operator to a target through a path the target does not verify.

Every role a tenant supplies MUST be assumed with the tenant's identifier as the condition value the role requires of its caller, presented by the platform rather than taken from the caller.

A refused assumption of a tenant's role MUST be tried again at the next call rather than recorded as permanently invalid.

The operator carried into a target's audit trail MUST be the outermost delegator of the session's chain.

The rendering of a principal id carried into an external audit trail MUST map no two principals to one value.

Where a target authorizes the platform's own identity rather than the operator, the per-operator limit MUST be enforced by the scopes of the call's assertion before the call is made.

## Human-Only Actions

### Actions Policy Reserves For People Defer

An action the tenant's policy reserves for a person MUST carry a call-type tag.

A substrate default MUST defer an action reserved for a person.

A call in the never-automate set is refused by a substrate check at the tool-call hook, as spec/core/governance.md specifies.

A change MUST record whom it was authored for.

A person who approves a change MUST NOT be in the change's authored-for set.

A defer on an action reserved for a person MUST have no automated resolver.

A defer on an action reserved for a person MUST have block as its timeout outcome.

Administration of a tenant's root key or a signing root MUST be an action reserved for a person.

An agent MUST NOT hold a credential that can administer a key.

## Audit

### Every Decision And Credential Is Audited

Every authorization decision MUST be published as an event that identifies the grant or policy that decided it.

An audit event MUST hold ids, principals, and decisions.

An audit event MUST NOT hold content.

Audit events MUST be kept apart from session data, with their own retention, so that destroying a
tenant's keys deletes its content but not the record of each action and its principal.

An authentication event MUST carry the verifier's request id when a verification ran, so that a refusal or a failure can be followed into the verifier's own records.

## The Substrate And Higher Levels

### Higher Levels Supply Providers, Policy, The Broker, And Flows

The mapping from external identities to principals MUST be supplied by an identity adapter at a higher
level.

Policy content MUST be supplied by the tenant rather than carried by the core.

The credential broker's backends MUST be supplied through one interface.

The identity adapter MUST write an operator's grant when the operator starts a session, capped by the operator's permissions and the role's upper bound.

### Signing Roots Are Held Apart From The Cluster

A lineage's signing root MUST NOT be created or administered by the cluster's own deployment.

A node MUST retry obtaining its signing grant rather than report itself ready without it.
