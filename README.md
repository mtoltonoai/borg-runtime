# Borg Runtime

Borg combines Fleet and Task Board. Their working code is maintained
here as ordinary source components. A new native executor runs agent sessions
directly against the Responses API.

| Component | Responsibility |
| --- | --- |
| `components/task-board` | Shared projects, tasks, agent definitions, documents, events and UI |
| `components/fleet` | Existing agent process supervision and workspaces |
| `crates/native-runtime` | Native Responses loop, independent sessions, durable conversation and tool recovery |
| `tooling/workspace_mcp.py` | Files and commands exposed over MCP for a configured workspace |

Fleet and Task Board retain their own manifests and lockfiles.
[Import provenance](components/SOURCES.json) records exact upstream revisions and
the three Fleet changes needed for the new directory layout.

## Native executor

Agent identities are separate from sessions: one identity can run multiple
independent sessions concurrently. Definitions are metadata, initially in a JSON
file, and reload at turn boundaries. Session state and transcripts live in
SQLite. Task Board remains the authority for shared work.

The native HTTP service provides session creation, status, transcript, steering,
stop, resume and uncertain-tool resolution, with a small dashboard at `/`.
Transient inference failures retry from committed conversation state. Startup
recovers interrupted inference; a tool interrupted without a durable result
requires resolution before another attempt.

The example uses **`gpt-6-astra`, `max` reasoning, 128000 maximum output tokens
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

Start each service in its own terminal from the repository root:

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

Run Fleet through `tooling/run-fleet`; see the
[Fleet README](components/fleet/README.md) for its CLI and existing Codex setup.

## Checks

```sh
tooling/check
```

The checks cover native recovery/concurrency and protocol behavior, workspace
MCP paths/processes, Fleet, Task Board, and the built Task Board frontend.
Component commands must run from their component directory.

## Next integration work

The native executor is the first engineering project. The initial implementation
provides the conversation engine and MCP access; Fleet's launcher and Task Board
metadata still need to become its integrated deployment and configuration path.
Existing Fleet/Codex sessions can build that integration.

Keep delivery incremental: wire one native session through the board and Fleet,
demonstrate parallel sessions and restart recovery, then have a session deliver
and deploy a tested change. Deployment must report the actual revision and
restart result. See [architecture](docs/architecture.md) for the boundaries.

## Independent durable recovery

`tooling/native_recovery.py` is a separately supervised Board/Fleet observer and
repair-task dispatcher. It detects missing execution paths, failed/uncertain
sessions and suspicious progress, preserves explicit waits and tool receipts,
and retries transient control-plane outages indefinitely. It never blindly
replays an original session. See [native recovery](docs/native-recovery.md) for
the metadata/API contract, persistent systemd unit, tests and safe live drills.
