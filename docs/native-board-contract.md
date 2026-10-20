# Native Board/profile and session contract

Task Board is the authority whenever `RuntimeConfig.board` is present. Host
configuration then needs connectivity/storage, not an identity list:

```json
{
  "database_path": "/var/lib/borg/native.sqlite",
  "listen": "127.0.0.1:8787",
  "host_id": "agent-lab-2",
  "runtime_url": "http://127.0.0.1:8787",
  "board": {"url": "http://127.0.0.1:8079", "timeout_ms": 10000, "report_interval_ms": 5000}
}
```

`board.bearer_token_env` optionally names the credential environment variable.
No credential value is stored in a profile, transcript, session or report. Local
`agents` remains a backwards-compatible bootstrap mode only when Board is absent.
The Responses API key and Codex login remain separate.

## Profile resolution

At every new inference boundary the runtime reads:

- `/api/agents/{id}` for identity, charter and lifecycle intent;
- `/api/agents/{id}/config/runtime?effective=true`;
- `/api/agents/{id}/config/tool?effective=true`.

Profile precedence is built-in defaults, enabled effective runtime entry
`profile` payload, recognized flat identity metadata, then `metadata.native`.
The Board merges role and agent entries by entry id; an agent's `profile` entry
replaces that role entry as defined by Board effective-config semantics. The
identity charter is prepended to optional profile `instructions`. No document or
old specification is needed. Paused/retired identities are not admitted.

Supported profile keys are `model`, `instructions`, `reasoning_effort` (`effort`
as an alias), `max_output_tokens`, `context_window_tokens`, `max_turns`,
`max_retries`, `retry_initial_ms`, `retry_max_ms`, `retry_jitter_percent`,
`provider_url`, `api_key_env`, `mcp_servers`, and `provider_tools`.
`profile_source` records Board authority and role/agent config versions; it is
runtime-produced, not an input override.

Defaults remain **gpt-6-astra / max / 128000 output / 1050000 shared context**,
with no input truncation and no default turn cap. `max_retries: null` means
unlimited transient retry and is the default. Legacy numeric `max_retries` is
accepted but never causes retry exhaustion. Local exponential delay and jitter
are bounded by `retry_max_ms`; a server `Retry-After` is a minimum that may exceed
that cap. Read-only discovery/profile transport failures retry indefinitely.
Permanent configuration/auth errors remain visible for owned repair. Unknown
mutating tool outcomes require reconciliation and are never replayed.

When effective tool entries exist, they are authoritative and replace legacy
profile tool arrays. Disabled entries grant nothing, including when all entries
are disabled. Supported unscoped enabled payloads:

```json
{"kind":"mcp","name":"board","url":"http://127.0.0.1:8079/mcp","tools_allowlist":["get_task","comment_task"]}
```

```json
{"kind":"mcp","name":"workspace","url":"http://127.0.0.1:8766/mcp/{session_id}"}
```

```json
{"kind":"mcp","name":"native","url":"{runtime_url}/mcp/sessions/{session_id}"}
```

```json
{"kind":"provider","tool":{"type":"web_search"}}
```

MCP payloads also accept `headers_env`. Unsupported/scoped grants fail closed;
there is no silent broad-tool fallback. Before discovery, `{session_id}` and
`{agent_id}` URL components and `{runtime_url}` are expanded. Placement can bind
the endpoint of an already-authorized `workspace` or `native` server; it cannot
add a revoked grant. Committed intents retain their bound original endpoints.

## Session creation and adoption

`Runtime::create_session_with_options(agent_id, task, input, options)` is async.
`SessionOptions` contains:

| Field | Meaning |
|---|---|
| `session_id` | Optional caller-allocated stable dispatch/idempotency key |
| `host_id` | Host binding; defaults to runtime host and must match it |
| `workspace_cwd` | Absolute per-session workspace path |
| `workspace_mcp_url` | Per-session endpoint for the authorized workspace server |
| `lifecycle_mcp_url` | Optional per-session native lifecycle endpoint |
| `generation` | Nonnegative Board generation; default 0, Fleet normally assigns 1 |
| `metadata` | Object; Fleet stores its dispatch id/request id here |

The service layer can expose these alongside `agent_id`, `task`, `input`. Exact
repeated creates return the existing session even while Board is unavailable or
the runtime is quiescing. Different intent under the same id conflicts. Creation
receipt, session and original input commit atomically. A completed/failed session
is not implicitly driven again. Multiple sessions under one identity have
independent transcripts, workspaces, MCP connections, stop flags and drivers.

`bind_session(id, options)` explicitly adopts bootstrap placement only with no
active driver or unknown/running tool. It never resets transcripts, changes
identity, reroutes committed tool receipts, or decreases generation. Already
committed pending intents whose grant/endpoint no longer matches become a known
non-executed tool error, not a write to a new workspace. Do not silently migrate
bootstrap sessions by changing identity workspace metadata.

## Control and reporting

- `load_agent`, `load_agents`, `session_agent` are async; `config()` exposes host
  infrastructure metadata to the HTTP/MCP adapter.
- `drain_session(id)` sets the durable stop flag without notifying/cancelling the
  current tool. A tool can safely request its own graceful stop and commit its
  receipt. Operator `stop(id)` retains explicit cancellation semantics.
- `quiesce()` closes admission and lets current tool receipts commit before
  parking work as ready. Poll `driver_running == false` before process restart.
  Quiescence never cancels a live tool. `unquiesce()` reopens admission; the
  service may drive ready sessions again. Retry backoff observes graceful drain
  promptly without changing unknown-write rules.
- The service must call `start_reporting()` once on startup. Library tests can
  explicitly call `flush_reports()` instead.

Session JSON retains the original status/task/error/turn fields and adds host,
workspace, generation, `phase`, `step`, `last_progress_at`, `driver_running`,
`failure_kind`, and `report_error`. Timestamps are Unix milliseconds. `step` and
`last_progress_at` advance only for durable model output, tool receipt or new
input—not retries/heartbeats. `driver_running` is recomputed from this process,
not trusted after a restart. A live inference/retry is not proof of a stall.

Every state update queues a durable local report. Delivery uses Board's existing
generation-fenced `/api/sessions/{id}/state` plus a separate agent config entry:
`config_kind=native_session`, `entry_id=session_id`, `payload=Session`. This is a
many-session catalog, never a singleton `agent.metadata.session_id`. Reports
retry after outages/restart, per-session delivery order is preserved, and a
failed report remains visible locally. Terminal sessions are not heartbeated
forever. A higher Board generation blocks a superseded runtime before a new
turn/dispatch. Fleet still owns placement/reservations and uncertainty after
host loss; this is not a cross-host filesystem replication protocol.

## Validation

```sh
cargo test --locked --workspace
cargo clippy --locked --workspace --all-targets -- -D warnings
BORG_TEST_TASK_BOARD_URL=http://127.0.0.1:8079 \
  cargo test --locked --test profiles real_board_role_profiles -- --ignored --nocapture
```

The opt-in live test creates a uniquely named non-Fleet test identity/role,
starts two sessions using a deterministic provider in an isolated local database,
changes role metadata, verifies live reload/reporting with unchanged host JSON,
and pauses the fixture identity on completion. It does not spend model tokens or
modify another agent's task/session.
