# Native Fleet contracts

The shared Task Board is the placement and dispatch authority. Native sessions
are not agent identities: two tasks (or explicit independent requests) for one
identity receive distinct session UUIDs and workspaces. `metadata.harness` equal
to `native` selects this execution path; the old `metadata.native` flag only
means a legacy Board-managed roster entry.

## Board registry

All paths below are relative to `/api`. Lists return plain JSON arrays.
Timestamps are server-generated Unix seconds.

- `POST /fleet/hosts/{host_id}/heartbeat`: `{capacity, runtime_url,
  workspace_root, runtime_ready, revision?, ttl_seconds?, metadata?}`.
  `runtime_url` includes `/api`. Capacity and TTL are seeded on first contact;
  later heartbeats update observations, not administrative policy.
- `GET /fleet/hosts`: includes `host_id`, `capacity`, `enabled`, `runtime_url`,
  `workspace_root`, `runtime_ready`, `revision`, `ttl_seconds`, `last_seen`,
  `fresh`, `active`, `metadata`. `fresh` requires an enabled host, a ready runtime,
  and a recent heartbeat. Unreachable is not proof of process death.
- `PATCH /fleet/hosts/{host_id}`: optional `capacity`, `enabled`, `ttl_seconds`,
  `metadata` (merge patch). Drain with `enabled:false` or `capacity:0`; existing
  sessions are not stopped. These controls survive worker heartbeat/restart.
- `POST /fleet/dispatches`: `{request_id, agent_id, task_id?, input?, metadata?}`.
  Without a task, input is required. With a task, the identity must be its owner;
  an omitted input generates an ordinary Board-oriented engineering kickoff.
  The same request and key return the same dispatch even when terminal. Changed
  intent returns HTTP 409. Only one active dispatch per task is allowed.
- `GET /fleet/dispatches`: optional `host_id`, `agent_id`, `task_id`, `status`,
  `request_id`, `session_id`, `limit`, `offset`. Default limit 100, maximum 1000;
  consumers must page to exhaustion. `GET /fleet/dispatches/{id}` reads one row.
- `PATCH /fleet/dispatches/{id}`: `{host_id,generation,version,status,progress?,
  error?,metadata?}`. Host, generation and version must match; stale reports get
  409. Each accepted report increments the version. The server projects the
  dispatch onto `task.metadata.native_dispatch` and emits status/error events.

Dispatch rows carry `id`, `request_id`, `session_id` (UUID allocated before any
side effect), `agent_id`, `task_id`, `host_id`, `runtime_url`, `workspace_cwd`,
`generation`, `version`, `status`, `input`, `progress`, `error`, `metadata`,
`created_at`, and `updated_at`. Creation also returns `created`.

Placement reserves capacity and the request key in the same transaction. It
selects the smallest active/capacity ratio among fresh eligible hosts with
stable host-id tie breaking. `agent.metadata.placement` and
`task.metadata.placement` constrain eligible hosts with `host`, `hosts`, or
`labels` (matching `host.metadata.labels`). The legacy observational
`agent.metadata.host` does not pin independent native sessions.

New workspace paths are exactly `<host.workspace_root>/<session UUID>`.
`metadata.workspace` is snapshotted from the task, otherwise the identity;
provisioning does not mutate an existing identity workspace. `metadata.native_attempt`
is captured from the task (default 1). Automatic task dispatch keys are
`task:{task_id}:attempt:{native_attempt}`. A completed session does not imply the
Board task is complete; recovery can create owned diagnosis instead of looping
another execution. A deliberate new attempt requires explicit task metadata.

Statuses are `reserved`, `provisioning`, `launching`, then native runtime states
`ready`, `running`, `retrying`, `waiting`, `stopping`, `stopped`, `completed`,
`failed`, `needs_resolution`; `cancelled` is available for a fenced reservation.
Completed, failed, stopped and cancelled release capacity. Uncertain tool
outcomes (`needs_resolution`) retain an explicit association and never replay
through provisioning. No reservation expires into another host merely because
a heartbeat or HTTP response was lost.

## Runtime boundary

Fleet calls `POST <runtime_url>/sessions` with `agent_id`, `task` (`task_N`),
`input`, caller-allocated `session_id`, `host_id`, `workspace_cwd`,
`workspace_mcp_url`, `generation`, and dispatch `metadata`. The runtime persists
that UUID before running. An exact retry returns the existing session without
replaying it; changed intent conflicts. Fleet first reads `/sessions/{UUID}`
when reconciling a lost create response, preserving completed tool results.

A shared, separately supervised workspace MCP server uses `--session-root` and
`/mcp/{UUID}` to bind each session to its existing UUID workspace. Restarting
Fleet does not require restarting this service or a native engine. Bootstrap
workspace services remain separate until their tools have completed.

## Bootstrap adoption

Before enabling a worker against existing tasks, record live sessions using
`POST /fleet/dispatches/adopt`: the create fields plus `session_id`, `host_id`,
`runtime_url`, `workspace_cwd`, optional `generation` and `status` (default
`running`). Register the host first. Use the canonical task request key and omit
input/metadata unless they are part of the intended create key. Adoption counts
existing capacity and preserves the original endpoint/path even when it differs
from the production runtime. It neither provisions nor restarts the session.

The integrator owns service rollout. Never stop a bootstrap process to collect a
pending tool result. Roll out the Board registry, adopt live sessions, then enable
workers; stage replacement engines and workspace services separately.
