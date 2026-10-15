# Core: Governance

> **CORE SPECIFICATION.** Behavior and invariants that hold under every outcome of every decision, free
> of implementation detail. This document defines the one governance mechanism: a fixed set of hook
> points, an ordered pipeline of deciders at each, a closed set of decisions, stop admission, and the rule
> that confidence comes from mechanism rather than from a human approval. Requirements realize
> [Core Principle VII](../../constitution.md), [Platform Principle P2](../../constitution.md), and
> [Protected Guarantee "Agents Never Approve"](../../constitution.md) and trace to
> [overview section 6](../overview.md).
>
> The capitalized key words are normative, with the meaning the constitution gives them. Each requirement is a single self-contained sentence carrying exactly
> one obligation, under a stable heading. The decider kinds' engines and the request size bound are
> declared defaults; the request and response shapes are pinned by the
> [decision-hook contract](../contracts/decision-hook.md).

## Purpose And Scope

Authorization, content deciders, approvals, secret scrubbing, stop admission, and interruption are one
mechanism so that the core has a single enforcement path. This document fixes the hook points, how a
decision request is built, how a pipeline runs, what each decision means, and how authority is widened
only by evidence. It states the mechanism's behavior, not the content of any tenant's policy. How authority is widened beyond sound checkers is a fixed default, checkers-only, recorded in [spec/record.md](../record.md) part A; its alternatives are the entry "Human Approvals And Record-Based Autonomy" in [spec/decisions/deferred.md](../decisions/deferred.md), and the requirements under the default and under each alternative are in spec/decisions/authority-widening/.

## The Hook Points

### The Core Defines A Fixed Set Of Hooks

The core MUST define a fixed set of hook points and enforce only at them.

The hook points MUST be the model request, the model output, the tool call, the tool result, the notification arrival, the compaction, the stop request, and the control operation.

The core MUST NOT add a hook point without a code change.

### Deciders Guard Inputs As Well As Outputs

The tool-result and notification-arrival hooks MUST guard what enters a session.

An inbound event never carries a tool call for the core to dispatch; spec/contracts/event-envelope.md pins that rule.

A tool MUST be dispatched only by a session's own model output.

## Decision Requests And Pipelines

### The Core Builds The Decision Request

At each hook the core MUST build a decision request carrying the principal and chain, the hook and call-type tags, the content, and the session's usage.

A decision request's configuration MUST state whether a truncation denies.

The request's truncation of content past the size bound, with an explicit marker, and its optional inclusion of the tool results a model output cites are pinned by spec/contracts/decision-hook.md.

The structured-secret check MUST read the full content regardless of the decision request's size bound.

A decision request on the notification-arrival hook MUST carry a bounded activity descriptor: the session's phase, the work invested in the current step as tokens and time, the expected time to the next turn boundary, and the session's priority.

A decision request MUST NOT carry session context beyond that descriptor.

### A Pipeline Runs Low-Cost Deciders First

A session's configuration MUST give an ordered pipeline of deciders per hook.

A pipeline MUST run lower-cost deterministic deciders before higher-cost ones.

A block MUST short-circuit the rest of its pipeline.

The effective pipeline at a hook MUST consist of the substrate's own checks, the tenant's governance deciders, and the definition's decider additions.

A substrate check MUST NOT be removable or reorderable by tenant or definition configuration.

Where the configured order and the cost order disagree, the cost order MUST prevail across cost classes.

Within one cost class, deciders MUST run in their declared order, the substrate's before the tenant's before the definition's.

A hook's outcome event MUST record the order in which the deciders ran.

### The Core Only Enforces

The core MUST enforce the decision a decider returns rather than form a decision of its own.

The closed set of decisions, allow, flag, block, defer, and transform, is pinned by spec/contracts/decision-hook.md.

## What Each Decision Does

### Allow And Flag

An allow MUST pass the subject on unchanged.

A flag MUST pass the subject on and record an event.

### Block

A blocked model output MUST be returned to the model with the block's reason rather than take effect.

A blocked tool call MUST become an error result to the model.

A blocked control operation MUST be rejected.

A block's reason never repeats the content it rejects, as spec/contracts/decision-hook.md pins.

A block's reason MUST pass the scrubbing decider, so that a block cannot put a secret back into context.

A tool spec blocked at load MUST NOT be loaded.

An event blocked at the notification-arrival hook MUST be applied as blocked, so that the inbox cursor advances past it.

The model MUST NOT see content blocked at the notification-arrival hook.

A block on any part of a response MUST discard the rest of that response ungated and unstored.

### Defer

A defer MUST hold the deferred call rather than the session.

A session with other work MUST continue while a call is deferred.

A deferred call's decision MUST arrive as an inbox event.

A session with no other runnable work while a call is deferred MAY be evicted.

A deferred tool call that is allowed MUST pass the loaded-definition and revocation checks again before it is dispatched.

A deferred tool call that is allowed MUST pass the tool-call hook's policy deciders again against the current grants before it is dispatched.

A deferred control operation that is allowed MUST be checked again against its target's current state and the current grants.

A deferred control operation that no longer applies when allowed MUST lapse with an event.

A defer's resolution, its timeout, and a deferred call's outcome are admitted past the inbox cap, as spec/core/sessions-and-partitions.md specifies.

The number of open defers a session holds MUST be bounded by a configured cap.

A new defer past the cap MUST be a block.

Resolving a defer MUST be a control operation that passes the control-operation hook.

### Transform

A transform MUST proceed with a stated patch, redacting spans or setting fields.

A secret in a tool result MUST be removed by a transform before the result reaches the model.

A transform that sets the model MUST select only a model the session's definition or the tenant allows.

A transform that sets the model MUST select only a model that accepts the transcript as it stands.

A transform's patch to a tool call's arguments MUST pass the tool's schema check before the call runs.

A decision event for a redaction MUST NOT record the content.

A redaction by a tenant or definition decider MUST apply to what the model is shown rather than to what a read grant holder reads.

### Each Decision Is Legal Only At Some Hooks

A configuration that lists a decider whose declared decision set includes a decision not legal at its hook MUST be rejected at validation.

A configuration that places a judge, a session decider, or an external decider synchronously on a latency-sensitive hook MUST be rejected at validation.

## Retries And Streaming

### A Retry After A Block Is Bounded

A retry after a block MUST be bounded by a budget.

A session whose retry budget is exhausted MUST block with the reason or escalate the block to an operator as
a defer.

How a retry after a blocked output is built depends on the execution decision; the requirements under each outcome are in spec/decisions/execution/.

Every tool use in a blocked attempt MUST receive an error result before the next request.

Spans a blocking decider marked MUST be redacted in every copy the platform stores or returns.

Where a provider binds reasoning content so that it cannot be edited, a secret found in it MUST block the output.

A retry after such a block MUST carry the reason without that attempt.

A block by a substrate check MUST NOT be escalated to a person.

Consecutive blocked cycles per session MUST be bounded by a configured cap across turns.

An attempt that takes an effect MUST reset the count of consecutive blocked cycles.

Allowing an escalated block MUST be recorded as an override of the last attempt.

### Output Is Gated Before It Takes Effect

The granularity at which model output is gated depends on the execution decision; the requirements under each outcome are in spec/decisions/execution/.

Model output MUST NOT take effect before it passes its deciders.

Anything that acts on an external system MUST be a tool call gated before it runs.

A live attach that sees output before it is gated MUST mark that output as ungated.

A model's reasoning content MUST pass the structured-secret check.

## Stop Admission

### A Blocked Stop Continues The Session

The stop hook MUST decide whether a session may stop or has remaining work.

A blocked stop's reason MUST become the next turn's input.

Consecutive blocked stops MUST be bounded, so that a session cannot be kept running indefinitely.

An external decider on the stop hook may run synchronously, as spec/contracts/decision-hook.md allows.

A stop request MUST be raised only when a turn ends with the provider's end-of-turn signal, makes no tool call, and leaves no unapplied inbox event.

The count of consecutive blocked stops MUST reset only on a cause the session did not originate.

A session whose blocked stop would exceed the bound MUST block with the reason rather than take another turn.

A session whose stop-admission decider failed MUST block with the reason and take no turn before its next inbox event or a resume.

## Confidence By Mechanism

An agent never holds a capability to approve a change, authority is widened only by a sound checker or by a person the tenant's governance designates, and a decision reaches a person only when no mechanism can decide it; constitution.md states these under the protected guarantee "Agents Never Approve" and Core Principle VII. That evidence is the checker's own record rather than an agent's account of it, and that a checker including a model never admits a change, are pinned by spec/contracts/evidence-record.md.

### Agents Produce Evidence And Never Approve

A checker that includes a model MUST be able to block or flag a change.

Weakening a property MUST be treated as a widening change.

A call in the never-automate set MUST be refused by a substrate check at the tool-call hook.

A call in the never-automate set MUST NOT be deferred.

A change's authored-for set MUST include every human principal in the session's chain and the requester.

An automated approver admitted by policy for agent-authored work MUST be a deterministic mechanism rather than an agent or a model judge.

A mechanism that gates an action on a person's decision MUST act only on a recorded positive decision whose fields match the action, never on the absence of a block.

### People Approve Policies, Not Instances

A class of change MUST be governed by a gate spec that states the evidence under which it may proceed.

A gate spec MUST be approved once.

A gate spec MUST run where the agent cannot access or modify it.

A gate spec MUST decide on reproducible evidence bound to the artifact.

## Shadow, Caching, And Episodes

### A New Decider Runs In Shadow First

A new or changed decider MUST run in shadow, recording without enforcing, before it may block.

A shadow decision MUST record whether it would have changed the outcome.

A shadow evaluation MUST NOT delay or change the hook's outcome.

A shadow decider MUST be evaluated even when an enforcing decider blocked first.

A shadow decider MUST see the request at the position it would hold in the pipeline, with the payload as the enforcing deciders before it left it.

A substrate check MUST NOT run in shadow.

A shadow decision's record of whether it would have changed the outcome MUST be computed by evaluating the hook's outcome again with the shadow verdict in place.

Promoting a decider from shadow to enforcing MUST carry its shadow evidence with the write that promotes it.

### Decisions Are Cached By Content

A stateless decider's decision MUST be cacheable by the decider version and the input's content hash.

A stateful or temporal decider's decision MUST NOT be cached.

A cached decision of a policy decider MUST be keyed by the versions of the policy set and of the grant snapshot it evaluated.

A decision served from the cache MUST be published as an event marked as cached.

A decision served from the cache MUST NOT be counted as a decider call.

### Every Decision Is An Event

Every decision MUST be published as an event.

A fail-retry-pass episode, a shadow result, and an operator override MUST be recorded as training labels.

A hook's decisions, their decision events, and the transition they gate MUST commit in one transaction under the partition's fence.

A re-driven evaluation after a crash MUST carry the same decision id as the lost one.

## Governance Changes Are Ordinary Operations

### Changing Configuration Passes A Hook

Every configuration change, including a session's own deciders, MUST be a control operation that passes
the control-operation hook.

The default policy MUST deny a session weakening its own governance.

A tool that writes a definition MUST declare the definition-write call type.

The tool-call hook MUST be able to deny a session writing its own definition or an ancestor's.

A narrowing or neutral change MAY apply by default.

A widening change MUST defer as a whole to the approver the tenant's governance designates, unless the
binding or grant under which the change arrives is trusted for that class of change.

A policy change MUST pass automated analysis before it can enforce.

A policy change declared as narrowing-only MUST prove it with a sound checker.

A configuration change MUST be classified as widening, narrowing, or neutral by comparing the effective configuration before and after it, each composed with the tenant's defaults in force at the time of the comparison.

A definition version that has no predecessor MUST be compared with the empty definition.

A field that no classification rule covers MUST be treated as widening.

Version metadata, such as the version number, the parent digest, and the list of restrictive fields, MUST NOT enter the comparison.

A longer idle time before a keyed instance ends, or the removal of an end condition, MUST be classified as widening.

A session principal MUST NOT write at the governance-policy tier.

The default policy MUST deny a session any governance-tier write whose scope reaches its own session, its definition, or an ancestor's definition, including the catalog entries those definitions reference.

Content the core produces or admits itself MUST carry a reserved call-type tag per kind, for a model's text output, a tool spec at load, and a definition version's content at admission.

A policy on the control-operation hook MUST be able to match on the operation's kind and tier.

## The Trust Root

### Two Paths Sit Outside The Pipeline

A tenant's bootstrap bundle MUST be the only configuration installed outside the governance pipeline other than through the emergency-override path.

Governance policy itself MUST change only through a tier above ordinary governance.

An emergency-override path, checked by the core itself and fully audited, MUST be able to replace a policy that blocks governance from operating.

A bootstrap bundle's digest MUST be recorded with the tenant.

Installing a bootstrap bundle MUST be an audit event.

A bootstrap bundle MUST NOT be installed again over a live tenant.

A bootstrap bundle MUST carry the tenant's governance-policy set, the grants for the emergency controls, the principals allowed to invoke the emergency override with their quorum and the override's expiry, the call-type tags of the never-automate set, and the tenant's defaults for the definition fields a version omits.

A definition field a version omits MUST take the tenant's default.

An emergency override MUST be invoked only by principals the bootstrap bundle lists, as many as the configured quorum.

An emergency override principal MUST be a person authenticated directly rather than attested by a service.

Each invoking principal MUST confirm the same replacement by its digest within the configured window.

An emergency override MUST only replace, remove, or restore a policy at the governance or governance-policy tier.

Each use of the emergency override MUST be an audit event that identifies the caller, the chain, the reason, and the replaced and new policies by digest.

Each use of the emergency override MUST alert every operator of the tenant through the tenant's registered destinations.

A replaced policy MUST be kept as a version.

A policy replacement made by emergency override MUST expire after the configured period unless it has been re-made through the governance-policy tier.

At expiry the replaced policy MUST be restored with an event.

A stage MUST refuse every governance-tier and governance-policy-tier policy write while its emergency-override path is not in service.

The module that implements a substrate check MUST be pinned by the platform outside any tenant's catalog.

### An Emergency Control Is Never Blocked By Policy

The grant check for an emergency pause, quarantine, or stop MUST be a substrate check that fails closed.

Once its grant check passes, an emergency pause, quarantine, or stop MUST NOT be blocked by a tenant policy or by a decider's failure.

A tenant policy MAY flag an authorized emergency pause, quarantine, or stop.

An emergency resume MUST be decided under the normal fail-closed rules.

### Decider Code Enters Through The Catalog

A decider's code, model, or weights MUST enter the platform only as a catalog entry written by a governance-tier operation and pinned by version.

A catalog entry MUST declare the decider's kind, its artifact by content id, the hooks it may run at, its call-type scope, the decision set it can return, what part of the request it reads, and whether it is stateless, stateful, or temporal.

A definition MAY carry decider data such as patterns, criteria, and policies.

A definition's decider addition MAY only narrow that entry's scope, tighten its failure outcome, or require that a truncation denies.

## Decider Kinds And Failure

### Deciders Are One Of A Closed Set Of Kinds

A decider MUST be one of a pattern, a schema, a classifier, a sandboxed program, a declarative policy, a model judge, a session decider, or an external decider.

A judge, a session decider, or an external decider on a latency-sensitive hook MUST run only as a defer.

A sandboxed program decider MUST be deterministic.

A sandboxed program decider MUST be bounded in time and memory.

A default decider MUST run behind the decider interface rather than as policy code built into the core.

A sandboxed program decider MUST run with no access to the network or the filesystem, as a pure function of the decision request and its state slot.

A tenant-supplied program decider MUST be evaluated in an instance that carries no state from any earlier evaluation.

A program instance MUST NOT be shared across tenants.

A program decider's module MUST import nothing beyond the decider interface.

A decider module or policy that fails validation MUST be refused when the configuration that references it is submitted, with the validator's diagnostics, rather than at the hook.

Every decider MUST have a deadline, configured with a default per decider kind.

A decider MUST be treated as failed on an error, a passed deadline, exhausted time or memory bounds, a decision not legal at its hook, or an unreachable destination.

A schema decider MUST validate the subject against its declared schema in process, allowing a subject that validates and denying one that does not with the validator's message as the reason.

A schema that does not compile MUST refuse the configuration that carries it.

A schema decider whose validator cannot run MUST count as the decider's declared failure handling.

A session decider MUST be an ephemeral session started to answer one decision request and ended when it answers.

A session decider's answer MUST be one of the closed set of decisions.

A session decider MUST be treated as a checker that includes a model.

### A Program Decider's State Slots

A state slot MUST be part of the session's durable state.

A state slot MUST be bounded per program by its catalog entry.

A session's state slots together MUST be bounded apart from the transcript head's bound.

A state slot MUST be read when an evaluation starts and written with the transition the hook gates.

A rollback MUST NOT remove a state-slot write.

A loop in a session's actions MUST be detected by a decider rather than by a function of an adapter.

### A Policy Decider's Verdicts Map Onto The Decisions

A deny from a policy decider MUST be a block unless every determining forbidding policy marks the deny as deferrable.

A deny with no determining forbidding policy MUST be a block.

An allow MUST become a flag when any determining permitting policy marks it as flagged.

### Failure Defaults To Closed

A decider's failure handling MUST default to blocking.

A classifier that is not required to block MAY run as a flag without blocking the turn.

A substrate check MUST NOT be configured to fail open.
