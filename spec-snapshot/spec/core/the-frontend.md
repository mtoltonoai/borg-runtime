# Core: The Frontend

> **CORE SPECIFICATION.** Behavior and invariants that hold under every outcome of every decision, free
> of implementation detail. This document defines the one authenticated API through which anything calls
> the platform: onboarding, governance, emergency controls, the data plane, the tail, webhooks in, and
> event hooks out. Requirements realize [Platform Principle P4](../../constitution.md),
> [Boundary "The Platform Hosts No Programs For Its Tenants"](../../constitution.md), and
> [Protected Guarantee "Identity Comes From Authentication"](../../constitution.md) and trace to
> [overview section 12](../overview.md).
>
> The capitalized key words are normative, with the meaning the constitution gives them. Each requirement is a single self-contained sentence carrying exactly
> one obligation, under a stable heading. The transport, the request caps, and the trace format are
> declared defaults.

## Purpose And Scope

The frontend is deliberately minimal: tools and hook destinations are services the platform calls, so the
surface that calls the platform is a narrow API scoped to a tenant. This document fixes who may call and
what the API accepts. Who owns the list of root sessions is a fixed default, imperative, recorded in [spec/record.md](../record.md) part A; its declarative alternative is the entry "Declarative Root Sessions And A Reference Definition Source" in [spec/decisions/deferred.md](../decisions/deferred.md), and the requirements under the default and under each alternative, including what the API accepts or refuses for a root session's configuration, are in spec/decisions/session-management/.

## Every Call Is Authenticated And Scoped

### A Principal Makes Every Call

Every call to the frontend MUST be made by an authenticated principal.

Every call MUST be scoped to one tenant.

Every call MUST pass the control-operation hook, so that an external caller meets the same policy a
session does.

A client of the platform MUST read and write session state only through the API rather than through the durable store.

Each route of the API MUST carry exactly one control-operation kind.

The tier of a control operation MUST be determined by the core from the operation's kind rather than supplied by the caller.

### A Forwarded Identity Comes Only From A Configured Proxy

A node whose proxy configuration is empty or unresolved MUST refuse every identity header.

A request whose forwarded identity is refused MUST be treated as unauthenticated, with a refusal event.

Traffic between an ingress proxy and the frontend MUST be encrypted.

## Onboarding

### Onboarding Installs The Bootstrap Bundle

A tenant's bootstrap bundle MUST be installed when the tenant is created.

## Governance

### Governance Operations Are Governance-Tier

Writing a policy, a grant, or a catalog entry MUST be a governance-tier operation.

A grant MUST be written by the identity adapter rather than by an arbitrary caller.

Binding, changing, or unbinding a webhook endpoint MUST be a governance-tier operation.

Binding a hook destination MUST be a governance-tier operation.

### A Deferred Operation Is Acknowledged With Its Decision

A control operation that a policy defers MUST be acknowledged as deferred together with its decision id.

The API MUST accept a read of a decision by its id that reports whether the deferred operation applied, lapsed, or was blocked.

### Operator Operations Are Admitted By Role

A platform-operator operation MUST be admitted only for a principal that holds a configured operator role rather than for every principal of the operator's account.

### Caps Are Configuration

Every operational cap MUST resolve at runtime from configuration rather than from a constant in code or in a node's configuration file.

A tenant MUST have a configurable cap on each of its sessions, its concurrent model steps, each session's queue depth, its message rate, and its topic groups.

A cap check MUST be atomic with the operation it bounds, so that concurrent requests cannot exceed the cap together.

A cap MUST be counted across the cluster rather than per node.

A request past a cap in force MUST be refused before any of its effects is applied.

A send to a session whose queue is at its depth cap MUST be refused with a retryable status that states the cap.

### Usage Is Counted Whether Or Not A Cap Is In Force

The frontend MUST count each tenant's usage of every capped quantity whether or not a cap is in force.

Each usage count MUST be published as an event carrying the tenant and the measure, so that a cap can be chosen from what tenants use.

### An Operator Of The Platform Sets A Tenant's Caps

A tenant's caps MUST be set for the tenant by an operator of the platform rather than by the tenant.

A cap set for a tenant MUST take effect on every node from the tenant's next request.

A request that sets caps MUST identify the tenant it acts on as its resource, distinct from the caller's tenant.

Every change to a tenant's caps MUST be published as an event identifying the tenant, the operator, and the caps.

A tenant MUST be able to read the caps in force for it.

## Emergency Controls

### Emergency Controls Work Without The Owner Of The List

The API MUST expose pause, stop, quarantine, and resume, per root session, per definition or template, and per tenant.

An emergency control MUST work when the owner of the list of root sessions is unreachable.

An emergency control MUST take effect at its commit without waiting on any store outside the cluster.

An emergency control MUST NOT be rate limited.

The frontend MUST accept an emergency control during a drain.

### An Emergency Control Takes Effect At A Safe Point

An emergency pause, quarantine, or stop MUST take effect at the current step's safe point rather than wait for a turn boundary.

A pause requested by the owner of the list of root sessions MUST take effect at the session's next turn boundary.

A model attempt abandoned by an emergency control MUST run again as a new attempt when the session resumes.

A tool call already dispatched when an emergency control takes effect MUST be left to finish under the fence.

An emergency control on a session MUST reach its sub-sessions and forks.

### A Control At Template Or Tenant Scope Reaches Every Partition

A template-scoped or tenant-scoped control MUST be recorded in a tenant-level record that identifies the control, its scope, the caller's principal and chain, the reason, the time, and its decision id.

A template-scoped or tenant-scoped control MUST reach every partition hosting the tenant without a poll.

A keyed instance created while a pause or quarantine holds at its scope MUST start paused or quarantined.

Each session's application of a control MUST be an event.

### The Effective Lifecycle Is The Most Restrictive

A session's effective lifecycle MUST be the most restrictive of the lifecycle its owner requests, the emergency controls in force at its session, its template, and its tenant, and any quarantine the platform placed.

The restrictiveness order MUST be run, then paused, then quarantined, then stopped or retired.

An emergency control MUST hold while no resume at its own scope has lifted it.

A change requested by the owner of the list of root sessions MUST NOT lift an emergency control.

A resume MUST NOT override a pause or a retirement the owner of the list requests, or the out-of-budget state.

### An Emergency Control Is Granted And Decided In Process

An emergency control MUST require a grant for that control at that scope.

An emergency control MUST be decided only by deciders that run inside the platform's own process.

A defer or a transform MUST NOT be returned for an emergency control.

## The Data Plane

### What The Data Plane Accepts

The API MUST accept a publish to a topic.

The API MUST accept a send to one session, or to a keyed template with its key, creating the instance
on first use.

The API MUST accept resolution of a defer, from a principal the defer allows.

The API MUST accept management of a session's dynamic subscriptions on its behalf.

The API MUST accept a question to a session answered through a fork that is discarded after it answers and does not modify the original.

The API MUST accept a read of a session's state, and of its transcript ranges with a read grant.

The API MUST accept creation, replacement, removal, and listing of a tenant's topic groups.

### Sessions Are Listed In Bounded Pages

The API MUST accept a listing of the caller's tenant's sessions in bounded pages.

A listing cursor MUST be valid only within the tenant that issued it.

A listing MUST reflect a session's creation and its removal from the commit that made each.

### A Read Of State Or Transcript Returns What The Grant Allows

A read of a blocked session's state MUST return the identity of the decider that blocked it and whether that decider answered a block or failed to answer.

A read of a session's state MUST NOT return content.

A transcript read MUST carry the compaction count its positions belong to, so that a reader detects a renumbering.

A transcript read through a read grant MUST omit reasoning content.

### Timer Operations Reach Only The Caller's Timers

The API MUST accept setting, re-arming, and cancelling a timer on a session from a principal holding a grant for it, reaching only the timers that principal set.

A timer operation the target has not applied within the call's deadline MUST be acknowledged as accepted.

### An Event Is An Opaque, Deduped Payload

An event accepted by the API MUST carry a topic, a priority, and an id that dedupes retries.

The envelope's other fields, its attachments, and the rule that an inbound event carries no tool call for the core to dispatch are pinned by spec/contracts/event-envelope.md.

### An Attachment Is Staged Before It Is Referenced

The API MUST accept an upload that stages an attachment's bytes under a holder owned by the tenant and returns a reference to it.

A staged holder MUST record the uploading call's principal and chain as provenance that grants nothing.

A publish or a send MUST refer to a staged upload only when the publisher's principal and chain both equal the recorded provenance.

A staged upload MUST be referable only within the configured window after the commit that recorded it, measured on the owner's clock.

A window on which a party other than the caller depends MUST be measured from a time the platform recorded on the owner's clock rather than from a time the caller supplied.

A publish whose attachments cannot all be resolved MUST append nothing.

An upload's dedupe record MUST last the life of its pin.

### An Upload Is Checked Before Its Body Is Read

Every check on an upload other than the comparison of the body's digest MUST run before any byte of the body is read.

An upload's caller MUST declare the digest of the body in a signed header before the body is sent.

An upload whose bytes do not match the declared digest MUST commit nothing.

A tenant's count of staged uploads MUST be reserved before any byte of the body is read.

A reserved slot MUST be released early only on a definite answer that the attempt committed nothing.

## The Tail

### A Tail Streams Gated Activity

The API MUST stream one session's activity as events that resume by id and can start from any point in
the transcript.

A tail MUST show output only once its deciders have passed.

A viewer of a tail MUST hold a read grant on the session.

A tail MUST be relayable from any node through a bounded buffer, so that a slow viewer is dropped and
never slows the session.

Live ungated output on a tail MUST be a privileged option marked as such.

A model's reasoning content MUST NOT be shown to a viewer, passed to a tool, or carried on an event, except on a tail under the privileged ungated option, marked as ungated.

A tail read MUST NOT change the session's state.

A tail read of a frozen session MUST NOT load the session.

### A Tail Token Opens One Stream

A tail token MUST expire no later than the configured assertion lifetime after it is issued.

The platform MUST end an open tail at its token's expiry with an end event that states the reason.

A tail token MUST travel only in a request header, never in an address or a log.

A tail token MUST NOT be stored.

A tail token MUST be bound to one viewer and one session rather than to a connection.

A tail token MUST have a type distinct from a call assertion, so that neither verifies as the other.

A tail token MUST open at most one stream.

The tail route MUST accept no credential other than a tail token.

Access to a tail MUST be decided at the control-operation hook when its token is issued.

### A Tail Position Survives Moves And Compactions

A tail position MUST identify the lineage epoch and the session's own entry counter, independent of the connection, the node, and the token that delivered it.

A tail position inside a frozen transcript range MUST stay resolvable from that range.

A tail position MUST survive a compaction, a freeze, an owner move, and a planned drain and restore.

A resume from a position the tail source still serves MUST continue with no event lost or repeated, under any valid token for the same viewer and session.

A viewer MUST be reset only for a position that is off the session's history, inside a range retention has removed, or above the head.

The reset MUST carry the current head rather than an error.

### A Tail Relay Runs Under The Platform's Identity

A relay of a tail between nodes MUST run under the platform's own identity with a scope limited to reading that session's tail.

Each relayed tail read MUST be audited with the relaying node, the tenant, and the session.

## Webhooks And Event Hooks

### Webhooks Let An External System Publish

A webhook MUST be an authenticated endpoint mapped to a principal.

A webhook endpoint MUST map to a service principal in its own tenant rather than to a person or a template.

A provider's own check on a webhook MUST NOT decide the principal.

A webhook endpoint's shared secret MUST be referenced by its configuration rather than held in it, readable only by the verifier.

A webhook MUST NOT refer to an attachment.

### A Webhook Delivery Is Deduplicated And Bounded

A webhook delivery MUST be deduplicated by the provider's delivery id for the endpoint's configured window.

A delivery with no id MUST NOT be deduplicated.

A webhook delivery past the endpoint's record cap MUST be refused with a retry delay rather than accepted by evicting an older record.

### Event Hooks Deliver A Session's Status

The API MUST deliver a session's lifecycle and status events to the destinations its definition specifies.

An event hook MUST NOT affect the session whose events it carries.

Destination registration, the delivery guarantees, the egress rule, and the receiver's obligations are pinned by spec/contracts/event-hook.md.

## Protocol Discipline

### Replies Are Legible And Traceable

A refusal MUST carry a stable error code that does not change with its wording.

A request over a size cap MUST be refused with a status that states the cap it exceeded.

The API MUST propagate distributed trace context, continuing a caller's trace or starting a new one.

Every reply the API returns and every event a call causes MUST carry the request id of that call.

A reply MUST echo the trace context it continued or started, including a refusal made before a route is matched.

### Every Refusal Has One Shape

A refusal of a path or a method no route serves MUST carry the same error shape and a stable code as every other refusal.

A refusal the frontend makes before a route is matched MUST be counted and published like a routed refusal.

A refused request body MUST carry a status that distinguishes an unsupported media type, a wrong shape, an oversized body, and an unparsable body.

### Refusal Classes Are A Closed Set

The refusal classes of the API MUST be a closed set.

Each refusal class MUST state whether a retry of the same request can succeed.

A refusal body MUST carry its class, the decision id where a decision gave it, and ids.

A refusal body MUST NOT carry content or a credential.

A throttled refusal MUST carry the delay after which a retry can succeed and the level of the limit that refused.

A refusal of a reference the caller is not permitted to use MUST be indistinguishable from the reference not existing.

A request that fails because a dependency of the platform was unreachable, throttled, or timed out MUST be answered with a retryable status distinct from a refusal of the request.

A refusal that follows from the caller's own configuration MUST be answered with a non-retryable status that states what was refused.

A request for a tenant whose configuration record supplies no capacity source MUST be refused with a configuration reason before anything runs.

### A Request That Changes State Is Deduplicated

Every request that changes state MUST carry a request id and the time of its first attempt.

A retry MUST repeat the request id and the first-attempt time of the first attempt.

A request whose id and body match a record within the dedupe window MUST be answered from the record rather than applied again.

A request whose first attempt is older than the dedupe window MUST be refused as stale rather than applied.

A request without a first-attempt time, or with one further ahead of the node's clock than the clock-skew bound, MUST be refused as invalid.

A request id reused with a body whose canonical digest differs MUST be refused as a conflict.

A publish or send id MUST be deduplicated per publisher principal and destination, so that the same id reused toward another destination is a fresh event.

### Every Route Has An Exact Grammar

The frontend MUST state the grammar of every path segment of every route.

The frontend MUST refuse a request whose path does not match its route's grammar exactly, with no percent-encoding and no trailing, empty, or doubled segment, before any route or grant check runs.

A route that takes no query MUST refuse a request that carries one.

A request MUST carry its tenant in exactly one place.

### A Request Completes Within The Connection Lifetime

A frontend request other than the tail MUST complete within the configured connection-lifetime bound.

Every long-poll bound and keepalive interval on the frontend's path MUST be below the connection lifetime.

### The Access Log Records Requests Without Content

The frontend's access log MUST record each request's authenticated principal, method, path, and response code, and no header or body beyond those.

The access log's retention MUST be at least the configured minimum.

A configuration change MUST NOT lower the access log's retention.

### Development Modes Stay On Development Stages

A development-only authentication or transport mode MUST refuse to start outside the development stage.

A stage left unset MUST be treated as deployed.
