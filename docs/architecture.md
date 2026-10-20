# Runtime architecture

Task Board is the collaboration and configuration authority. Fleet supplies
placement, isolated workspaces and persistent workers. The native executor runs
the Responses API conversation loop. These components are source directories in
this repository, not submodules.

An agent is a stable identity with configurable instructions, model and tools.
A session is an independent conversation belonging to an agent. Several sessions
can use the same identity concurrently on different hosts without sharing
transcripts, drivers, stop flags or workspaces. Tasks belong to Task Board and
can link to several sessions. A session records its task link rather than
copying the task's ownership or lifecycle; finishing a session does not complete
the Board task.

## Board metadata and placement

In the deployed path, host configuration contains connectivity and storage, not
an identity list. At each inference boundary the runtime reads the Board agent's
charter, lifecycle intent, effective `runtime/profile` and effective `tool`
entries. Role entries are inherited with agent overrides. A new identity needs
Board metadata such as a role and `harness: native`, not host JSON or per-identity
environment edits. Local JSON-only profiles remain a standalone development
mode, not the deployed authority. See the [profile contract](native-board-contract.md)
and [metadata setup](native-metadata-deployment.md) for precedence and grants.

Persistent Fleet workers run on `agent-lab`, `agent-lab-2`, `agent-lab-3` and
`agent-lab-4`, all using the one Board on `agent-lab`. Board transactions reserve
capacity, request identity, session UUID and placement together, choosing the
least occupied capacity ratio among fresh eligible hosts. Capacity, enablement
and placement constraints are metadata. Observational `agent.metadata.host` is
not a placement rule. The retained builder runtime is not a fifth Fleet worker.

Assigned tasks and Board-mode native lifecycle `session_create` requests go
through Fleet's broker. Each new session receives a UUID workspace with its own
Git metadata and work branch. Reconciliation reuses the original dispatch and
workspace instead of creating another attempt after an uncertain response.
Session-bound MCP exposes workspace and lifecycle tools; the runtime binds Board
attribution from the durable session identity. Tool grants may authorize other
identities without changing the caller. See [Fleet](native-fleet.md) and
[lifecycle tools](native-lifecycle-tools.md) for the exact interfaces.

## Durable execution and recovery

Per-host SQLite stores conversation items, tool intents/results and reporting
outboxes. Model output commits before tools execute. Completed results and
original tool endpoint bindings survive restart; new calls use the session's
explicit current binding. Restart can retry interrupted inference from durable
input. An interrupted tool with no durable result requires reconciliation of the
original effect before recording its outcome. It is not replay permission.

The HTTP service wakes sessions on creation, steering, explicit continuation
and startup recovery. A stopped session stays stopped until explicitly resumed.
Only one driver executes a particular session at a time. Graceful drain waits
for the active tool receipt; immediate cancellation can leave an unknown result.

Runtime reports are durable, generation-fenced and cataloged per session on
Board. Fleet separately reports reservations, host placement and observed
runtime progress. Board's `/native` view consumes these projections; it is not
another task or transcript authority. Reporting retries after outages, and
heartbeat freshness is not evidence of useful model progress.

The separately supervised recovery controller on `agent-lab` observes Board,
Fleet and native execution. It creates owned, deduplicated repair tasks and
independent triage sessions, rather than blindly stopping or replaying original
work. Its persistent journal prevents duplicate effects after restart. Recovery
starts without a readiness dependency on the services it watches and retries
transient control-plane outages indefinitely. A slow live inference is not
proof of death; explicit pauses and verified external waits remain visible.
Repairs require a cause, durable fix and regression evidence. See
[independent recovery](native-recovery.md).

## Model and retry profile

The selected profile is `gpt-6-astra` with `reasoning.effort=max` and
`max_output_tokens=128000`. The context window is 1050000 tokens; input,
instructions, tools, reasoning and generated output consume that shared window.
With the full output reservation, approximately 922000 tokens remain for input.
No additional smaller input allowance or default turn cap is imposed.

Requests use stateless Responses with the complete retained conversation and
reasoning items, and disable automatic truncation. Provider credentials are
resolved from environment references, not stored in profiles or transcripts.
The Responses API credential is separate from Codex login.

The deployed profile uses `max_retries: null`, `retry_initial_ms: 5000`,
`retry_max_ms: 60000` and `retry_jitter_percent: 20`. Transient inference and
retryable read-only profile/discovery failures retry indefinitely from committed
state. Local exponential delay and jitter are capped at 60 seconds; server
`Retry-After` floors may exceed that cap. Retry state remains visible and
operator stop interrupts inference/backoff. Permanent configuration errors and
unknown tool writes remain owned repair/reconciliation cases, not endless
mutation retries.

## Deployment and acceptance

The integrated path is implemented and persistently deployed, not a future
Fleet/Board integration. [Deployment](native-deployment.md) records the
four-worker rollout and repeatable build, staging, drain and activation commands.
The integrator owns shared publication and service activation; sessions deliver
tested commits from their isolated branches. Releases identify selected source
and actual running images, and verify original receipts before work resumes.

Implemented behavior and enabled services do not establish all live acceptance.
The final Git-route, Board revision/projection and parent-child recovery-lineage
checks remain tracked in project 1 under task 6 and their owned repair tasks.
Do not infer completion from this architecture description. The
[acceptance audit](native-acceptance.md) requires concrete placement, isolation,
recovery, receipt-preservation and native-authored deployment evidence.
