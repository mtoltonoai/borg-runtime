# Runtime architecture

Task Board is the collaboration product. Fleet supplies workspaces and process
supervision. The native executor supplies a Responses API conversation loop.
These components are source directories in this repository, not submodules.

An agent is a stable identity with configurable instructions, model and tools.
A session is an independent conversation belonging to an agent. Several sessions
can use the same identity concurrently without sharing transcripts. Tasks belong
to Task Board and can link to several sessions. Session state records its task
link rather than copying the task's ownership or lifecycle.

The initial executor loads agent definitions from a JSON metadata file and
refreshes them at turn boundaries. This is the bootstrap definition source;
Task Board configuration is the intended shared source as integration proceeds.
MCP is the tool boundary for both Task Board and workspace tools.

The local session database stores conversation items and tool operations.
Model output is committed before tools run; completed tool results are retained
and returned to the model. Restart can retry interrupted inference from durable
input. An interrupted tool operation with no durable result requires resolution
before it can be repeated. A session's status and error remain visible.

The HTTP service wakes sessions on creation, steering, explicit continuation and
startup recovery. A stopped session stays stopped. Independent sessions can run
concurrently while a single session has one active execution.

## Model profile

The selected profile is `gpt-6-astra` with `reasoning.effort=max` and
`max_output_tokens=128000`. The model's context window is 1050000 tokens; input,
instructions, tools, reasoning and generated output consume that shared window.
With the full output reservation, approximately 922000 tokens remain for input.
No additional smaller local input allowance is imposed.

Requests use stateless Responses with the complete retained conversation and
reasoning items, and disable automatic truncation. This makes local state the
recovery source. Response chaining can be added without changing the separation
between identities and sessions.

Sources verified on 2026-10-20:

- [Astra model limits](https://developers.openai.com/api/docs/models/gpt-6-astra)
- [Reasoning state](https://developers.openai.com/api/docs/guides/reasoning)
- [Function calling](https://developers.openai.com/api/docs/guides/function-calling)

## Delivery target

The first engineering project is this native executor. Existing Fleet/Codex
agents can build it before it is capable of running those agents itself. The
demonstration is two sessions under one identity, useful workspace work through
MCP, recovery after a transient provider failure or process restart, visible
progress, and a tested change delivered to a running service.

Source checks, review results and deployed revisions should be linked plainly.
Deployment should build the selected source, distribute its artifacts, restart
the selected services and report the actual result. Role and deployment choices
are metadata rather than compiled workflow prose.
