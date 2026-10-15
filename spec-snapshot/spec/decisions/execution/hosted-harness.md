# Outcome: Hosted Harness

> **OUTCOME SPECIFICATION.** These requirements apply only when the decision record adopts the hosted-harness outcome of decision [execution](../execution.md); they add to the core and never replace it. This document defines how the platform runs an agent harness the operator already operates as a session's executor: the harness as a supervised process, its working state in the platform's stored form, its credentials by lease, its model calls through the egress point, its tool calls through dispatch, its events at step boundaries, its reports through the API, and gating at the boundary the platform sees. Requirements realize [Platform Principle P5](../../../constitution.md), [Core Principle V](../../../constitution.md), and [The Platform Hosts No Programs For Its Tenants](../../../constitution.md) and trace to [overview section 18](../../overview.md).
>
> The capitalized key words are normative, with the meaning the constitution gives them. Each requirement is a single self-contained sentence carrying exactly one obligation, under a stable heading. The grace period after a lifecycle signal and the per-harness capacity lease are declared defaults.

## Purpose And Scope

Under this outcome a session's model loop runs inside a harness: a program of the operator's that builds model requests, reads replies, calls tools, and keeps its own context. The platform does not run that loop. It owns everything the harness crosses to act: the process lifecycle, the stored form, the credential broker, the egress point, dispatch, the inbox, and the API. Every invariant of the core holds because each is enforced at one of those boundaries rather than inside the harness. The constitution forbids the platform to execute tenant-supplied code outside the sandbox, so the harness process runs on a host the tenant operates and the platform drives it through a host service, in the same way a tool server fronts the tenant's runners.

The unit of progress is the step: the harness runs from one step boundary to the next, and at each boundary it commits its working state through the platform and receives the events that arrived. Within a step the harness is opaque to the platform; at the boundary it is not.

## The Harness Is A Supervised Process

### The Platform Starts And Ends The Process

The platform MUST start one harness process for a session when the session has work and none is running.

A harness process MUST serve exactly one session.

The platform MUST start a harness process only on a host the tenant operates, through a host service, and never on a platform node.

A host service MUST verify the platform's assertion and the session's lease generation on every start, signal, and end it receives.

### Lifecycle Controls Map To The Process

A pause, stop, or quarantine of the session MUST be signaled to its harness process by the platform, so that lifecycle controls work without the harness's cooperation.

The platform MUST end a harness process that has not reached a step boundary within the configured grace period after a pause, stop, or quarantine signal.

A harness process whose session's lease generation has advanced MUST be ended by the host service that runs it.

### Exits Are Lifecycle Events

A harness process that exits MUST be reported as a lifecycle event of the session, carrying its exit class.

A harness process that exits with a failure MUST be restarted from the session's last committed step within the session's retry budget.

### The Partition Bounds The Processes

When a partition needs capacity, it MUST end the harness processes of idle sessions lowest priority first.

## Working State Lives In The Stored Form

### Each Step Commits Through The Platform

A harness MUST write its working state at each step boundary through the platform, in the stored form the stored-form contract pins.

A step MUST be acknowledged to the harness only after its working state has committed.

A harness process MUST be started from the session's stored working state rather than from any state left on a host.

### Working State Is Checked Before It Is Stored

A harness's working state and reported transcript MUST pass the structured-secret check before they are stored.

## Credentials Come Only By Lease

### No Long-Lived Credential Reaches The Harness

A harness MUST obtain a model credential only as a lease from the platform's credential broker, bound to one attempt.

A long-lived credential MUST NOT be present in a harness process's environment, configuration, or working state.

A harness's credential for the API MUST be a lease bound to the session's lease generation, so that a superseded process cannot report or commit.

## Model Calls Leave Through The Egress Point

### The Egress Point Is The Only Path

Every model call a harness makes MUST leave through the platform's egress point.

A harness process MUST reach no destination other than the egress point, dispatch, and the API.

### One Usage Record Per Attempt

The egress point MUST write one usage record per model attempt it carries, whether or not the harness reports the attempt.

Every attempt MUST carry an attempt id that the egress point assigns or verifies, so that a retry is a new attempt.

Where the harness's report and the egress point's record of an attempt disagree, the egress point's token counts MUST be the ones metered.

### The Egress Point Enforces Policy And Caps

The egress point MUST run the model-request hook for every attempt.

The egress point MUST refuse an attempt addressed to a region, model, or data-retention term the tenant's policy does not permit.

The egress point MUST apply the tenant's rate caps per model to a harness's attempts.

An attempt the egress point refuses MUST be answered with a structured error that states the cap or policy it violated.

## Every Tool Call Passes Dispatch

### Dispatch Is The Only Tool Server

A harness MUST be configured with the platform's dispatch as its only tool server.

Every tool call a harness makes MUST be dispatched as the core requires: resolved against the loaded definition, passed through the tool-call and tool-result hooks, recorded, and leased its credentials by reference.

The platform's native tools MUST be served to the harness by dispatch as any other tool.

## Events Arrive At Step Boundaries

### The Platform Delivers And Wakes

Inbox events MUST reach the harness only through the platform, at a step boundary.

The platform MUST start or signal the harness process when an event is applied to its session's inbox.

A notification whose priority exceeds the session's interrupt threshold MUST be signaled to the harness process, so that the harness ends its step at its next safe point.

A configuration change MUST be delivered to the harness at a step boundary as a platform event.

### A Step Boundary Has Nothing In Flight

A step boundary MUST be a point at which the harness has no model call and no tool call in flight.

## The Harness Reports Through The API

### Reports Carry The Session's Identity

A harness MUST report its transcript, outputs, and usage to the platform through the API under the session's identity and lease generation.

### Reports Become Transcript Entries And Observation Records

A step's working-state commit MUST include the step's transcript delta, so that the tail and transcript reads cover every committed step.

A model attempt the harness reports MUST be recorded as an observation record that points at the transcript range of the attempt.

A compaction the harness performs MUST be reported as a compaction, with the transcript renumbered and the compaction count advanced, so that tail positions and transcript reads hold.

## Gating Applies At The Boundary

### Output Is Gated Where It Leaves The Harness

Model output MUST be gated at the boundary where it leaves the harness: a tool call, a published or sent event, or an output returned through the API.

The model-output hook MUST run on each output the harness reports before the output is stored or shown on the tail.

A block at the boundary MUST be returned to the harness as an error result with the block's reason.

## Scheduling Is At The Step Level

### The Platform Schedules Steps And Bounds Capacity

The platform MUST schedule harness processes by session priority at the step level.

The platform MUST bound a harness process by a capacity lease that states its concurrent model attempts, its memory, and its processor share.

### The Loop Inside A Step Is The Harness's

The platform MUST NOT compact, trim, or re-render a hosted session's transcript.

## The Harness Is Tenant-Supplied Code

### The Program Is Governed As Configuration

A harness program MUST be addressed by its digest in the session's configuration, so that what runs is what was governed.

A session configuration that refers to a harness program not on the tenant's trusted list MUST be refused at validation.

## Simulation

### A Simulated Harness Exercises The Same Boundary

In simulation the harness MUST be replaced by a simulated harness that starts, reaches step boundaries, calls dispatch, calls through the egress point, and reports through the API.

A scenario MUST show that an attempt the harness does not report still has a usage record.

## What This Outcome Does Not Provide

Under this outcome the platform has no compaction of its own, no management of the prompt cache, no interrupt threshold applied within a turn, no construction of a retry after a blocked output, and no gating of a reply block by block. Each of those is the harness's: the harness compacts its context, keeps or lets expire its cached prefix, decides when to stop within a turn, and builds the retry after a refusal. The platform's guarantees under this outcome are the ones stated above, each enforced at a boundary the harness cannot avoid.
