# Borg Runtime

Borg combines one shared Task Board, persistent Fleet workers and a native
Responses API executor. Their working code is maintained here as ordinary source
components. Board metadata defines identities and execution profiles; Fleet
places independent native sessions in isolated workspaces.

| Component | Responsibility |
| --- | --- |
| `components/task-board` | Shared projects, tasks, agent definitions, documents, events and UI |
| `components/fleet` | Native host capacity, durable dispatch and isolated session workspaces |
| `crates/native-runtime` | Native Responses loop, independent sessions, durable conversation and tool recovery |
| `tooling/workspace_mcp.py` | Files and commands exposed over MCP for a configured workspace |

Fleet and Task Board retain their own manifests and lockfiles.
[Import provenance](components/SOURCES.json) records exact upstream revisions and
the three Fleet changes needed for the new directory layout.

## Native executor

Agent identities are separate from sessions: one identity can run multiple
independent sessions concurrently, including on different hosts. The runtime
loads charters, lifecycle intent, effective role/agent runtime profiles and tool
grants from Task Board at inference boundaries. New identities need Board
metadata, not a host JSON entry or per-identity environment edits. Task Board is
the shared work and configuration authority; per-host SQLite retains independent
session transcripts, tool receipts and reporting state.

The native HTTP service provides session creation, status, transcript, steering,
stop, resume and uncertain-tool resolution, with a small dashboard at `/`.
Transient inference failures and retryable read-only MCP discovery failures retry
indefinitely from committed state. `max_retries: null` is the explicit unlimited
policy (and default); legacy numeric `max_retries` values are accepted but never
terminate recoverable work. `retry_initial_ms`, `retry_max_ms` (default 60000),
and `retry_jitter_percent` (default 20) control bounded exponential sleeps;
`Retry-After` seconds or HTTP dates are a minimum wait, even beyond the local
backoff cap. Retry stage, count, error and next delay are durable and visible. Operator
stop interrupts both inference and backoff. Permanent configuration/authentication
errors remain visible for repair, and uncertain tool writes are never retried. Startup
recovers interrupted inference; a tool interrupted without a durable result
requires resolution before another attempt.

The selected profile uses **`gpt-6-astra`, `max` reasoning, 128000 maximum output tokens
and the full 1050000-token context window**. Input and output share that window:
reserving all output leaves approximately 922000 tokens for input, including
instructions, history and tools. There is no smaller local input allowance,
silent context truncation, or default session turn limit. Provider rate limits
still depend on the API account.

Provider credentials are read from the environment variable named in metadata;
they are not stored in the session database. Codex authentication used by Fleet
is separate from the native Responses API key.

## Build and run

Use Rust 1.99 or newer, Python 3.10 or newer, and Node 22 or newer.

```sh
cargo build --locked --bin borg-native
cd components/fleet
cargo build --locked --bin fleet
cd ../task-board
cargo build --locked
cd web
npm ci
npm run build
cd ../../..
```

### Board-managed deployment

The deployed path is Board metadata -> Fleet reservation -> isolated workspace
and native session. Fleet workers run persistently on `agent-lab`, `agent-lab-2`,
`agent-lab-3` and `agent-lab-4`, using the shared Board on `agent-lab`. An independent
recovery controller also runs on `agent-lab`; it is not tied to a native session's
lifetime. Host placement, capacity, roles, tools and recovery policy are metadata.

Use the [persistent deployment guide](docs/native-deployment.md) for the
infrastructure configuration, build/stage/activation commands and recorded rollout
state. [Board metadata setup](docs/native-metadata-deployment.md) defines profiles
and new identities; [Fleet](docs/native-fleet.md) provisions assigned tasks and
[lifecycle MCP](docs/native-lifecycle-tools.md) creates independent concurrent
sessions. These are the implemented execution path, not future integration work.

### Local standalone development

The JSON-only example below is for an isolated development setup, not the
production identity source or a four-host placement test. Do not start a second
Board for the deployed fleet. In a separate local setup, start each service in
its own terminal from the repository root:

```sh
tooling/run-task-board
python3 tooling/workspace_mcp.py --root "$PWD" --port 8766
# OPENAI_API_KEY must already be available in this process's environment.
target/debug/borg-native --config config/runtime.example.json
```

Open Task Board at `http://127.0.0.1:8079` and native sessions at
`http://127.0.0.1:8787`. Each service stores its state under `.runtime/`.

Create two independent sessions for the same identity by submitting this request
twice with different task/input values:

```sh
curl -sS http://127.0.0.1:8787/api/sessions \
  -H 'Content-Type: application/json' \
  -d '{"agent_id":"engineer","task":"native-executor","input":"Inspect the repository and identify the next small integration improvement."}'
```

`config/runtime.example.json` enables web search and the Task Board/workspace
MCP servers. Remove or replace those entries when using different tools. The
workspace helper runs commands as its user; use a dedicated workspace/process
boundary when isolation is required. Services bind to loopback by default.

For Board-managed execution, use Fleet's persistent `native-worker` command
and the configuration described above instead of posting directly to a chosen
runtime. The [Fleet README](components/fleet/README.md) also covers its separate
Codex tooling.

## Checks

```sh
tooling/check
```

The checks cover native recovery/concurrency and protocol behavior, workspace
MCP paths/processes, Fleet, Task Board, and the built Task Board frontend.
Component commands must run from their component directory.

## Deployment evidence and remaining acceptance

The integrated Board/native/Fleet path is implemented and deployed across the
four workers. Board's `/native` view consumes per-session reports and Fleet host
and dispatch data; it does not replace the runtime's durable transcripts. See
[architecture](docs/architecture.md) for component and authority boundaries.

Implementation and enabled services are not final acceptance. Project 1 / task 6
owns the remaining live Git-route, Board revision/projection and parent-child
recovery-lineage verification. The [acceptance audit](docs/native-acceptance.md)
requires actual revisions, load distribution, independent same-identity sessions,
filesystem isolation, preserved receipts across restart, owned repairs and a
native-authored tested change deployed and published. Missing evidence remains
unproven; this documentation does not close those checks.

## Independent durable recovery

`tooling/native_recovery.py` is a separately supervised Board/Fleet observer and
repair-task dispatcher. It detects missing execution paths, failed/uncertain
sessions and suspicious progress, preserves explicit waits and tool receipts,
and retries transient control-plane outages indefinitely. It never blindly
replays an original session. See [native recovery](docs/native-recovery.md) for
the metadata/API contract, persistent systemd unit, tests and safe live drills.
