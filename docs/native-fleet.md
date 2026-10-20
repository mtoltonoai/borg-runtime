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

## Persistent worker

Build with `cd components/fleet && cargo build --locked --bin fleet` (Rust 1.99)
and run `fleet --config /absolute/native-fleet.toml native-worker`. See
`config/native-fleet.example.toml`. `--once` performs a bounded reconciliation,
waits for that pass's provisioning jobs and reports observed session state; it
is useful for smoke checks, not the persistent service command.

`fleet spin-up <identity> --apply` routes `metadata.harness=native` directly to
the Board broker. It does not consult legacy bootstrap documents, trust prompts
or a tmux harness. The old watchdog excludes these identities. Native stop/drain
controls target independent sessions, not an identity-wide tmux kill.

The worker uses a process-scoped lock under `workspace_root`. It reads the Board
agent catalog and actionable tasks, atomically reserves new attempts, polls
its assigned dispatches, provisions UUID directories, probes the independent
workspace MCP route, then creates the native session with its reserved UUID.
Git setup runs in background jobs, so cloning cannot hold the heartbeat loop.
Completed or uncertain engine outcomes are never sent back to provisioning.
Runtime reachability failures retain their status/placement, and retrying live
inference retains its capacity instead of being mistaken for a dead worker.
Health `ready:false`/`draining:true` removes a runtime from new placement.

REST `/tasks` currently returns a **complete, unpaginated list** even when
`limit`/`offset` are supplied. The worker reads each task-status group once.
`/agents` and `/fleet/dispatches` are genuinely paginated and are read to
exhaustion. These different contracts must not be collapsed into one generic
pagination loop: the old WIP assumption would stall forever at 100 tasks.

Workspace creation uses a staging directory and an exclusive ownership marker,
then renames into the final UUID directory. Repository clones have independent
Git metadata, branches and working files (`--no-local --no-hardlinks`). Existing
workspaces with a matching manifest are reused without checkout/reset/clean;
unowned or partial trees are retained as visible provisioning failures for
repair, never discarded. The marker is excluded from source commits. The
native profile (model, full context, output tokens, reasoning and turn policy)
remains in Board/runtime metadata; Fleet does not introduce another limit.

For a user service, enable `Restart=always`, use an absolute Fleet binary/config
path and `KillMode=control-group`. Only transient Git provisioning children
belong to Fleet's service. The native engine and session workspace helper must
be separate persistent services; restarting Fleet must not interrupt their
active tool calls. The integrator owns the install/enable/start commands and
safe bootstrap adoption sequence.

Runtime observations are projected into `dispatch.metadata.runtime`:
`phase`, `step`, `updated_at`, `last_progress_at`, and `driver_active` (from the
engine's `driver_running`). Native engine timestamps retain their native
millisecond units. `metadata.observed_at` and Board `updated_at` are seconds and
indicate observation freshness, **not** actual model progress. Top-level
`progress` and `error` are carried without declaring a slow inference dead.

## Regression evidence

- `cd components/task-board && cargo test --locked`: atomic multi-worker
  placement, weighted capacity, freshness/drain, changed-intent/CAS conflicts,
  adoption and database restart, independent same-identity workspaces, and a
  3MiB request crossing the real REST boundary (64MiB transport allowance,
  matching native rather than an accidental 1MiB/2MiB broker allowance).
- `cd components/fleet && cargo test --locked -p fleet`: existing Fleet checks
  plus native route/intent, payload stability, actual independent Git clones,
  idempotent user-edit preservation, symlink/traversal rejection and locking.
- `python3 tests/test_native_fleet.py`: **real Board and four Fleet processes,
  actual Git and workspace MCP services**, with a fault-injecting native HTTP
  fixture. Eight tasks under one identity reserve two per host. A lost response
  after create causes no duplicate; a loss before commit retries the same UUID.
  Each actual workspace tool runs once. Board/Fleet restarts preserve bindings
  and edits; killing Fleet during a workspace shell tool does not interrupt it.
  Bootstrap adoption/drain and visible owned provisioning failure are checked.
  A 105-task backlog finishes scheduling at capacity and advances all four
  heartbeats on the next pass instead of rereading the same list forever.

The integration fixture is not evidence that a real model ran on four machines;
that is a separate live deployment acceptance owned jointly with the integrator.

## Four-host connectivity and shared scope

Use `python3 tooling/native_routes.py render --out <staging-dir>` to generate
per-host Fleet TOML, native infrastructure merge patches and **new** route units.
Rendering never installs or activates a service. Generated shared-worker config
omits `project_id`: new projects and identities become eligible via Board
metadata, without host edits. `--project` is an explicit isolated-test option.
The process regression adds a second project/identity after first dispatch and
verifies both run without any subsequent host configuration changes.

| Target runtime | Native `runtime_url` origin | Fleet REST base |
| --- | --- | --- |
| agent-lab | `http://127.0.0.1:17871` | `http://127.0.0.1:17871/api` |
| agent-lab-2 | `http://127.0.0.1:17872` | `http://127.0.0.1:17872/api` |
| agent-lab-3 | `http://127.0.0.1:17873` | `http://127.0.0.1:17873/api` |
| agent-lab-4 | `http://127.0.0.1:17874` | `http://127.0.0.1:17874/api` |

The same loopback aliases must resolve to the **same remote hosts from every
lifecycle caller**, not only from the builder. The existing worker
`borg-board-tunnel.service` owns local Board8079 and reverse native17872/3/4 on
the controller. Leave it untouched while tools are active. Add these independent
units, only after verifying their listeners are unused:

- Controller `borg-native-controller-route.socket` and matching `.service`:
  `127.0.0.1:17871` to controller-local `8787` via systemd socket proxy.
- Worker2/3/4 `borg-native-routes.service`: persistent SSH local forwards
  `17871` to controller8787; `17872/3/4` to the controller's existing reverse
  listeners; and `17870` to the existing builder/bootstrap reverse listener.
  Uses the already installed `/home/dev-user/.ssh/borg_config` and key, with
  keepalive/restart and `ExitOnForwardFailure`. No new credentials or public
  listeners, and no changes to the existing Board/runtime/workspace units.

Install the rendered new units into `/etc/systemd/system`, `daemon-reload`, then
`enable --now` the controller **socket** or the worker **route service**. Keep
activation ownership explicit with the integrator; only the independent routing
units may be activated without a native/Board drain. Native binary, Board,
workspace and Fleet service activation remain the integrator's coordinated job.
Merge native config fields recursively, preserving database and credential
settings, including existing `board.bearer_token_env`.

`python3 tooling/native_routes.py audit` SSHes to every lab (executes locally for
the current lab), verifies the caller hostname, and probes the full 4x4 health
matrix plus each lab's builder17870 route. It fails for a missing route rather
than inferring availability from the controller. `--require-identity` additionally
requires integrated health responses to report the correct target `host_id`;
legacy health without that field proves transport only. Run `audit-local` from
the builder to prove its existing forward access to all four targets without
copying its keys. Save JSON with `audit --output <evidence-path>`.
