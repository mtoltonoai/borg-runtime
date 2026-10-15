# Outcome: Direct Model Turns And Scheduling

> **OUTCOME SPECIFICATION.** These requirements apply only when the decision record adopts the direct outcome of decision [execution](../execution.md); they add to the core and never replace it. This document defines how every model call is scheduled, prioritized, and routed; how capacity is shared across a cluster; and the properties of the inference layer that renders a session's transcript for a provider and decodes its reply. Requirements realize [Platform Principle P1](../../../constitution.md) and [Core Principle III](../../../constitution.md) and trace to [overview section 18](../../overview.md).
>
> The capitalized key words are normative, with the meaning the constitution gives them. Each requirement is a single self-contained sentence carrying exactly one obligation, under a stable heading. Providers, their endpoints, and their limits are declared defaults.

## Purpose And Scope

Colocating many sessions on a node lets one scheduler see every waiting turn. This document fixes what that scheduler does: order by priority, turn throttling into queueing, lease quota across the
cluster, route across capacity sources, and keep each session's route sticky. It also fixes the inference
layer's invariants: a provider-neutral transcript, opaque provider state, a request that is a pure
function of the log, aligned cache boundaries, and structured error classification.

## Every Call Goes Through A Scheduler

### No Call Is Made Directly To A Model Endpoint

Every model call MUST pass through a scheduler on the partition owner's node.

A session MUST NOT call a model endpoint directly.

### Priority Follows The Work

A session's effective priority MUST be the highest of its base priority, the priority of the work it is
handling, and any priority inherited from a session waiting on it.

A session's effective priority MUST return to its base priority when the work that raised it is done.

Raising a session's priority MUST be a granted capability checked at the control-operation hook.

A definition's base priority MUST be bounded by the upper bound of the binding or grant under which the definition arrived.

### Priority Orders Everything That Queues

Model admission MUST be ordered by priority.

The order in which a partition processes its dirty sessions MUST be by priority.

Eviction, when a partition needs memory, MUST remove the lowest-priority idle sessions first.

The deciders' compute pool MUST serve by priority.

## Throttling Becomes Queueing

### Admission Adapts To Capacity

A scheduler MUST admit waiting turns per model and endpoint in priority order, breaking ties by deadline.

A throttle MUST narrow the admission window rather than fail a session that is inside its deadline.

A scheduler MUST admit against the model endpoint's known request and token budgets rather than discover them by throttling.

### Quota Is A Cluster Resource

Quota shares MUST rebalance by demand.

A share of quota MUST be reservable for a priority class, so that urgent work is admitted while
background work saturates the rest.

A model call MUST NOT wait on a quota-coordination call.

### Fair Share

A scheduler MUST weight admission per tenant and, within a tenant, by priority, from configuration.

Budgets MUST be applied at the model-request hook.

## Latency Classes

### Three Classes

The platform MUST offer interactive, background, and batch latency classes.

A batch request's result MUST arrive as an inbox event, so that its session can be evicted while it
waits.

Compaction, distillation, and retrospectives MUST default to the background or batch class.

## Capacity Sources

### A Source Is An Account, A Region, And A Model Endpoint

A capacity source MUST be an account, a region, and a model endpoint, reached through a role.

A scheduler MUST route across capacity sources by spare capacity and cost.

### A Tenant's Own Capacity Comes From Tenant Configuration

A capacity source MUST come from the tenant's configuration rather than from an agent definition.

A tenant-supplied role MUST be used only if it is on the tenant's trusted list.

Every role a tenant supplies is assumed with the tenant as the external id, as spec/core/identity-delegation-and-secrets.md requires.

A transform MUST NOT set the capacity source that serves a model.

A scheduler MUST route a session's model call only to a region the tenant's policy permits.

A node's permission to call a model endpoint MUST be limited to the models and endpoints its capacity sources list.

A turn MUST NOT start for a tenant whose configuration supplies no capacity source.

### Routes Are Sticky

A session's route to a capacity source MUST be sticky, so that its cached prefix stays warm.

## Connections

### Connections Are Shared And Bounded

The transport MUST enforce a provider's per-connection concurrency limit before sending rather than
discover it by refusal.

The transport MUST enforce the configured request body cap before a body is sent.

## The Inference Layer

### One Provider-Neutral Transcript

A session's transcript MUST be held in one provider-neutral form from which each provider's request is
rendered.

A provider-bound payload MUST be carried opaquely, tagged with its provider, and interpreted only by that provider's adapter.

Provider state such as reasoning or a compaction item MUST be round-tripped unmodified.

### A Request Is A Function Of The Log And Policy

A model request MUST be a pure function of the session's durable log and its policy.

A rendered segment MUST depend only on its content, the provider's wire format, and a render version, so that it is cached by content and shared across sessions.

### Cache Boundaries Coincide

A cache breakpoint MUST be placed at a segment boundary, so that the client's render cache and the
provider's prompt cache invalidate the same suffix.

A breakpoint that no block can carry MUST be dropped and reported rather than fail the turn.

### Changes To The Prefix Preserve The Cache

A change to the cached prefix MUST be made through the provider's cache-preserving path or at a
compaction.

The system prompt MUST NOT be edited in place outside a compaction.

### A Model Switch Never Fails A Turn

A switch of model mid-session MUST NOT fail a turn because of reasoning produced by another model.

A tool call made under one provider and answered under another MUST render as a valid call-and-result pair under the provider that renders the request.

### Reasoning Binding Is Explicit

Where a provider binds reasoning to the history it was produced in, the platform MUST set the binding
policy explicitly rather than rely on the provider's default.

### Failures Are Classified Structurally

A provider failure MUST be classified from the provider's structured fields rather than by matching
message text, except where a provider offers no structured field for the condition.

A context-window overflow MUST be classified as such, so that the turn loop compacts rather than retries.

### Retention Terms Are Honored

A scheduler MUST route a session only to a model whose data-retention terms the tenant's policy permits.

### The Platform Keeps Each Model's Facts

The platform MUST keep, for every model it routes to, the model's context window, its maximum output, and whether it supports tool use, streaming, reasoning, prompt caching, and image input.

A session configuration that requires a capability the model's record lacks MUST be refused when the configuration is submitted.

A model id whose provider the platform does not know MUST be refused when the configuration is submitted.

## Instrumentation

### Every Turn Publishes Its Outcome

Every turn MUST publish one outcome event carrying its request id, model, stop or failure class,
latencies, and token totals.

Every stage of a turn MUST be an event at a level matching its frequency.

A turn abandoned by its caller MUST be reported with the stage it reached.

### Metrics Split By Closed Sets

Turn metrics MUST split only by model, wire format, endpoint, signing mode, error class, stage, stop
reason, segment kind, and cache lifetime.

Every token count MUST be split by model.

### Scheduling Is Observable

A scheduler MUST publish queue depth, wait time, admitted concurrency, and throttles per model and model endpoint.
