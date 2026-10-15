# Outcome: Direct Execution

> **OUTCOME SPECIFICATION.** These requirements apply only when the decision record adopts the direct outcome of decision [execution](../execution.md); they add to the core and never replace it. This document lists where the direct outcome's requirements live and carries the few requirements that other core documents could not keep without presuming that the platform runs the model loop. Requirements trace to [overview section 18](../../overview.md).
>
> The capitalized key words are normative, with the meaning the constitution gives them. Each requirement is a single self-contained sentence carrying exactly one obligation, under a stable heading.

## Purpose And Scope

Under the direct outcome the platform owns a session's model loop: it builds each request from the durable log, calls the provider through its scheduler, decodes the reply, gates each completed block, dispatches the tool calls the reply makes, compacts the transcript, and manages the prompt cache. The requirements that describe that loop are in two documents in this directory:

- [direct-model-turns-and-scheduling.md](direct-model-turns-and-scheduling.md) states how every model call is scheduled, prioritized, and routed; how quota is leased across the cluster; how capacity sources are chosen; and the properties of the inference layer that renders the transcript for a provider and decodes its reply.
- [direct-context-compaction-and-cache.md](direct-context-compaction-and-cache.md) states how the model's view is derived from the durable log, how a transcript is compacted and frozen, and how the platform manages the prompt cache.

The sections below carry the sentences that the core documents on events, governance, failure handling, and sessions state only under this outcome. Each sits here under its own heading so that the core keeps a pointer sentence in its place.

## Interruption Within A Step

### The Threshold Is Computed Within A Step

The interrupt threshold MUST be computed from the session's state, the work already done in the current step, the expected remaining duration of the step, and the session's own priority.

### Interrupting A Turn Cancels The Stream

Interrupting a model turn MUST cancel the stream and discard the partial output.

## Gating Inside The Loop

### Output Is Gated Per Block

Model output MUST be gated per completed block.

### A Retry Carries The Blocked Attempt

A retry after a blocked output MUST carry the blocked attempt up to the refused block, followed by a platform-authored message with the block's reason.

## Model Failures The Loop Handles

### A Context Window Exceeded Compacts And Retries

A model attempt that exceeds the context window MUST cause a compaction followed by a retry of the turn.

A session whose compaction cannot bring the prompt under the context window MUST end blocked with the reason.

### A Failure Mid-Stream Discards The Reply

A model reply that fails mid-stream MUST be discarded in full.

## Prefix Changes

### A Prefix Change Is A Mid-Conversation Instruction

A prompt or policy change MUST be applied as a mid-conversation instruction.
