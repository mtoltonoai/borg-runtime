# Core: Tools And Dispatch

> **CORE SPECIFICATION.** Behavior and invariants that hold under every outcome of every decision, free of implementation detail. This document defines the loaded definition as the tool registry, the dispatch of a tool call as a durable operation, the native tools the platform implements itself, sub-sessions and forks, and the rule that everything else is a service. Requirements realize [Boundary "The Platform Hosts No Programs For Its Tenants"](../../constitution.md) and [Platform Principle P6](../../constitution.md) and trace to [overview section 5](../overview.md).
>
> The capitalized key words are normative, with the meaning the constitution gives them. Each requirement is a single self-contained sentence carrying exactly one obligation, under a stable heading. The wire shapes are pinned by the [tool-call contract](../contracts/tool-call.md); the tool protocol is a declared default.

## Purpose And Scope

Tools are services, and the agent's definition lists them. This document fixes what the platform does
with that list: refuse anything not on it, gate and record every call, lease credentials by reference,
commit results as state, and recover by each tool's semantics. It also fixes the small set of native
tools and the shape of sub-sessions and forks. Where agents run code is a fixed default, none (agents run no code of their own, and every tool they call is a service), recorded in [spec/record.md](../record.md) part A; its alternatives are the entry "Where Agents Run Code: Existing Runners Or The Workspace Service" in [spec/decisions/deferred.md](../decisions/deferred.md), and the requirements under each alternative are in spec/decisions/workspace-service/.

## The Definition Is The Registry

### Dispatch Refuses What The Definition Does Not List

A definition MUST list the tool servers, and optionally the tools on each, that its sessions may call.

Dispatch MUST refuse a call to a server the session's loaded definition does not list.

Dispatch MUST refuse a call to a tool the session's loaded definition excludes.

The platform MUST NOT rely on a provider to enforce which tools a model has loaded.

The platform MUST NOT keep a registry of tools apart from the definitions.

A tool name listed by more than one server MUST be published as an event identifying each server and which one is offered.

A listing whose tool names do not fit the providers' name constraints MUST be refused with the reason.

A configuration whose declared tools share a name MUST be refused when submitted.

A configuration that states a protocol revision the platform does not implement MUST be refused with the revisions the platform implements.

A native tool MUST be offered to the model only when the session's configuration lists it.

### Specs Are Loaded, Hashed, And Checked

The platform MUST hash each tool spec a session loads.

A dispatch record MUST carry the hash of the spec the model saw and the build the server reported.

A tool spec MUST pass the input deciders when it loads.

A call's arguments MUST be checked against the spec's schema at the tool-call hook.

The arguments sent to a tool MUST be the arguments that passed the schema check, so that a transform's patch is checked before the call runs.

### Changes Reach Sessions As Reconfiguration

A server's announced change to its tool list MUST reach running sessions as live reconfiguration, within what the loaded definition allows.

The platform MUST list a server's tools again on every reconnect.

Adding a server to a definition MUST be a widening change.

A revocation of a tool MUST apply at dispatch, so that a revoked tool cannot run even from a turn
already in flight.

A tool call in flight when its tool is removed MUST run to completion under its dispatch record.

The result of a call whose tool was removed while it ran MUST be committed to the transcript.

A call the model makes to a tool the current configuration does not list MUST be answered with an error result rather than fail the step.

A transcript MUST remain renderable for every provider after a tool it references is removed.

A step whose server fails to list its tools MUST run with the server's last listing rather than fail.

A connection or listing request to a tool server MUST be retried within the call's deadline, honoring a retry-after when one is given.

Every retry of a tool-server request MUST be an event carrying the server, the attempt, and the reason.

## A Tool Call Is A Durable Operation

### The Dispatch Sequence

A tool call MUST be resolved against the session's loaded definition before anything else.

A tool call MUST pass the tool-call hook before it is recorded.

A dispatch record MUST be committed before the call is sent.

Credential leasing for a tool call is specified in spec/core/identity-delegation-and-secrets.md.

A tool call's output MUST be streamed while it runs.

A tool result MUST pass the tool-result hook before the model sees it.

A tool result MUST be committed as a state transition.

A streamed chunk of a tool's output MUST NOT be committed as a result of its own.

A blocked tool result MUST reach the model as an error result that states the call completed and its result was withheld, with the reason.

A page of a stored tool output MUST pass the tool-result hook before the model sees it.

A tool result whose staged output is unavailable MUST be recorded as outcome unknown.

A server's refusal for a missing credential, its refusal for insufficient scope, and its refusal of a superseded owner MUST be three distinct results.

A refusal for insufficient scope MUST reach the model as an error result that states the scope required.

A tool call's deadline MUST NOT be set later than the configured upper bound after the call's issue.

Every call the platform makes to a tool server MUST carry the session's distributed trace context.

### Recovery Follows The Tool's Semantics

An in-flight idempotent call found on recovery MUST run again.

A side-effecting call interrupted before its outcome was recorded returns an unknown outcome that states what was sent and when, as spec/contracts/tool-call.md pins.

A tool with a reconcile binding MUST be asked on recovery whether its effect took effect.

The platform MUST NOT present a call id to a service, as a re-issue, a close, or a reconcile, after the call's cutoff.

A call's cutoff MUST be its first dispatch's deadline plus the configured outcome-unknown window, less a margin of the assertion lifetime plus twice the clock-skew allowance.

A re-issue of a call id MUST NOT move the call's cutoff.

A re-issue's own deadline MUST be cut to the cutoff.

A call whose outcome is not known at its cutoff MUST be recorded as outcome unknown rather than as not run.

An outcome-unknown record MUST stay matchable for an identical re-issue through the call's cutoff.

An outcome-unknown record MUST NOT stay matchable past the compaction that drops its turn.

A result returning to the platform MUST NOT be refused because it carries an older lease generation.

### Calls Leave Through One Egress Point

Every outbound connection the platform makes, to a tool server, a hook destination, a decision-hook destination, or any other registered destination, MUST leave through the egress point.

The egress point MUST admit only the destinations the tool's binding lists.

The egress point MUST check a destination's resolved address at every connection, in every address form, so that a host name that resolves to a refused address is refused too.

The egress point MUST refuse a destination that resolves to a loopback address, a link-local address, an unspecified address, the host's own service range, a private range, or an address in the cluster's own network ranges, unless the tenant's configuration allows that range on the development stage.

A destination given as a numeric literal in a non-canonical form MUST be refused when the configuration is submitted.

A connection the egress point refuses MUST be published as an event carrying the destination and the address.

The egress point MUST NOT follow a redirect off the registered destination.

The egress point MUST verify the certificate of every destination it admits.

A destination inside a refused range MUST be admitted only when it is registered by name for the request's use, resolves into the address range the deployment records for that internal path, and presents a certificate that verifies for that name.

A destination registered for one use MUST be admitted only for a request of that use.

### Dispatch Is Observable

Tool metrics MUST split only by executor kind and outcome.

The tool's name is carried in events as a field, as Core Principle III in constitution.md requires.

A tool server the operators run MUST continue the caller's trace.

A tool server the operators run MUST carry the call's trace id on every event it publishes for the call.

## Native Tools

### Native Tools Act Only On The Platform's Own State

The platform MUST implement natively the tools that act on its own state: subscribe and unsubscribe, publish and send, start a sub-session, fork a session, end a child, wait for a child's result, list a session's children, set the interrupt threshold, the timer operations, the key-value slot, loading a deferred tool the definition already lists, and paging a stored output.

The platform MUST NOT implement natively a tool that does not act on its own state.

A function that does not act on the platform's own state MUST be provided by a service the definition lists.

### Mechanisms Are Not Tools

The platform MUST NOT require a model turn to report status, drain an inbox, re-arm a loop, send a
heartbeat, or request a compaction.

### The Sandbox Is Not A Tool Executor

The sandboxed program runtime MUST be used for deciders rather than as a tool executor.

## Executors

### Three Executor Kinds

The platform MUST support native tools, tool-protocol servers, and plain request-response servers as executors.

### What Every Service Receives

Every service called on a session's behalf receives the session's assertion, as spec/contracts/signed-assertion.md and spec/contracts/tool-call.md pin.

A service MAY send into a session in its own tenant only while that session has dispatched to the service, on the service's own topic and at a bounded priority.

A service MAY exchange a call's assertion for credentials of its own at the credential broker.

A tool server MAY deliver a result larger than the payload bound by staging it through the API as an attachment that its result event refers to.

A tool server's right to stage an attachment MUST be bound to one open dispatch to that server, identified by the session and the call id in fields the server signs.

The platform MUST check that the dispatch is open to that server before it accepts any byte of the staged output.

The answer to that check MUST reveal only whether the call is open to that server.

A staged output MUST be usable only by the result of the dispatch that admitted it.

A session's own publish or send whose payload exceeds the bound MUST be refused at the tool-call hook with an error result that states the bound.

### A Server May Ask The Platform

A tool server's request for a model completion MUST run as a model call of the calling session, under its deciders and its budget, or be refused.

A tool server's request directed at a person MUST be delivered as a question to the session's principal with a deadline, or be refused.

A tool server's request the platform does not support MUST be refused with a protocol-level error rather than left unanswered.

A tool server's log and progress notifications MUST be published as events of the calling session.

## Sub-Sessions

### A Sub-Session Is Never Wider Than Its Parent

A sub-session MUST run its parent's definition narrowed by the call, or another of the tenant's
definitions as a keyed instance with the parent as requester.

A sub-session MUST NOT hold authority its parent lacks.

A sub-session's budget is its parent's, allocated as spec/core/identity-delegation-and-secrets.md requires.

A parent's deciders MUST apply to its child in addition to the child's own.

A child's model MUST be one its parent's configuration allows.

A child's depth and count of sub-sessions MUST be bounded by its parent's remaining allowance.

A request to create or reconfigure a child beyond its parent's authority MUST be refused at the call with the reason.

### A Sub-Session Reports To Its Parent

A sub-session's lifecycle events MUST arrive in its parent's inbox.

A sub-session's final output MUST return as the result of the call that started it.

A child's end MUST be delivered to its parent's inbox in the same commit that ends the child.

## Forks

### A Fork Starts From Its Parent's Transcript

A fork MUST take its parent's transcript up to the fork point and its parent's definition narrowed by
the call.

A fork MUST diverge from the fork point without modifying its parent.

A fork's first request MUST follow its parent's route, so that forking costs a cache read rather than a
rewrite.

A fork MUST pin its parent's frozen blobs and copy only the unfrozen head.

A fork's grants and budget MUST be its parent's, narrowed.

### A Fork Is Its Own Principal

A fork's assertion identifies the fork rather than its parent, as spec/contracts/signed-assertion.md pins.

A fork MUST inherit none of its parent's timers.

### Fork Queries Are Discarded

A fork started to answer a question about its parent MUST run with model-only tools.

A fork started to answer a question MUST be discarded once it answers.

A fork query MUST leave the original session's state unchanged.

### An Experiment Fork Is Intercepted

A principal with a grant MUST be able to fork a session at any step of its transcript.

An experiment fork MUST route every tool call to the interceptor its experiment designates rather than to the tool's server.

A tool call on an experiment fork MUST pass the tool-call hook before it reaches the interceptor.

An experiment fork MUST NOT act on an external system.

An experiment fork's transcript MUST be readable as a tail by the experiment's principal.

An experiment fork MUST be discarded when its experiment ends.

### Recurring Work Uses A New Sub-Session

The sub-session tool SHOULD direct recurring work, and work that needs only a short instruction rather than the parent's transcript, to a new sub-session rather than to a fork.

## Long Tool Runs

### A Session May Be Evicted While A Tool Runs

A call may return before its work completes and deliver its result later as an inbox event, as spec/contracts/tool-call.md pins.

A session MUST be evictable while a long tool run is in progress.

A call that received a non-final result MUST keep its dispatch record open while its final result has not arrived and its deadline has not passed.

A call that answered with a non-final result MUST NOT be run again at its deadline.

At its deadline, a call that answered with a non-final result MUST be reconciled or reported as outcome unknown, by its semantics.

A dispatch record MUST stay matchable to a late result, by call id, through the call's horizon.

A final result that arrives after the horizon MUST be dropped with an audit event.

A late result for a call that already has a recorded result MUST be dropped as a duplicate.

A late result for a call reported as outcome unknown MUST be delivered to the model as a notification identifying the call.

A session's open dispatch records and its outcome-unknown records MUST each be bounded by a configured cap.

A model output that asks for more calls than the cap leaves free MUST receive an error result for each extra call that states the cap, before any dispatch record commits.

At the outcome-unknown cap a side-effecting call MUST be refused with an error result while no record has passed its cutoff.

Reaching the outcome-unknown cap MUST raise an alert.
