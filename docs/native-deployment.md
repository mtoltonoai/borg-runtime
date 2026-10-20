# Persistent native deployment

The shared coordination authority is the existing Task Board on `agent-lab`.
Never create a second Board database on a worker. Native transcripts are separate,
per-host execution state; Fleet's placement/dispatch rows live in the shared Board.

## Paths and services

Default state root: `/home/dev-user/.local/share/borg`.

| Unit | Hosts | Responsibility |
| --- | --- | --- |
| `borg-board` | `agent-lab` only | Existing `state/board.sqlite`, loopback 8079 |
| `borg-board-tunnel` | other labs and builder | Persistent existing SSH connection to that Board |
| `borg-native` | all four labs | Existing `state/native.sqlite`, loopback 8787 |
| `borg-workspace` | all four labs | Legacy/bootstrap checkout MCP, loopback 8766 |
| `borg-session-workspace` | all four labs | Session-isolated MCP under `workspaces/{session_id}`, loopback 8791 |
| `borg-fleet` | all four labs | Board-authoritative capacity, dispatch, workspace and runtime reconciliation |
| `borg-recovery` | `agent-lab` only | Independent durable incident/outbox controller |

The former controller/worker/deployer services stay disabled. The installer
rejects activation if it finds these services active or enabled. Credentials stay
in existing mode-0600 environment files and are neither packaged nor printed.
Responses API credentials and Codex login remain separate.

## Build and stage

From a clean committed checkout:

```sh
# Locked dependency builds; independent component caches, without debug-symbol bloat.
# BORG_BUILD_PROFILE=release is also supported; default dev builds are debug=0.
tooling/build-native-release
revision=$(git rev-parse HEAD)
release="$PWD/.runtime/releases/$revision"
python3 tooling/native_release.py verify --release "$release"
python3 tooling/deploy_native.py "$release" --hosts \
  borg-agent-lab borg-agent-lab-2 borg-agent-lab-3 borg-agent-lab-4
```

Staging transfers only whitelisted build outputs and supporting tools. The
manifest identifies the exact clean source commit and SHA-256 of every file.
Destination releases are immutable; a repeated transfer is accepted only when
all hashes match. Staging alone never restarts a process or edits a database.
Set `BORG_BUILD_ROOT` to use a dedicated build cache when disk space is tight.

## Configuration contract

Host JSON contains only runtime infrastructure (`database_path`, `listen`,
`board`, `host_id`, `runtime_url`), not a hand-maintained list of identities.
Board identity charter, effective runtime profile and tool grants supply identity
configuration. The selected model remains `gpt-6-astra`, max reasoning, 128000
output, shared 1050000-token context, and no default turn cap.

Transient inference and read-only discovery retry indefinitely with visible
status, capped jittered backoff and Retry-After. Until the explicit unlimited
runtime is deployed, legacy bootstrap profiles use `max_retries=4294967295` and
`retry_initial_ms=5000`. This is a compatibility bridge, not a finite-budget
product policy. Operator stop and uncertain-write reconciliation remain effective.

Fleet starts with:

```sh
fleet --config /home/dev-user/.local/share/borg/config/native-fleet.toml native-worker
```

Its top-level `board_api` is `http://127.0.0.1:8079/api` and `host` identifies the
lab. `[native]` supplies `runtime_url` (ending `/api`), `workspace_root`,
`workspace_mcp_url = "http://127.0.0.1:8791/mcp/{session_id}"`, `capacity`,
`poll_seconds`, `project_id`, `repo_url`, and `repo_ref`. Capacity is then Board
metadata, not a host-local scheduling authority. Do not enable dispatch before
existing bootstrap sessions are adopted or their tasks have a visible terminal
or maintenance disposition. A stable identity can have multiple independent
session UUIDs/workspaces.

## Guarded rolling activation

1. Coordinate a bounded, visible maintenance/drain with the task/session owners.
   The installer stops local Fleet admission before changing its runtime or tools.
2. **Do not call the old bootstrap stop API during a tool.** It cancels receipt
   collection and leaves an unknown side effect. The installer automatically sets
   the existing durable stop flag without notifying cancellation and waits for
   the original execution boundary, including any raced tool receipt. It never
   infers dispatch exclusion from progress text. A live slow model call is not
   proof of a dead process. No operator
   approval or agent-completion gate is required.
3. Resolve any truly uncertain tool outcome from original receipts, not by
   repeating the write. A durable drain journal owns every deployment pause and
   resumes original work after verification (including after recoverable errors).
   Timeout cleanup stays resume-pending until the runtime actually clears its
   stop flag. Detached systemd jobs use --no-block; a self-targeted calling tool
   returns its receipt instead of polling its own drain.
   Unknown outcomes remain visible and are never replayed.
4. Configure the existing state paths, metadata and credentials. Then activate
   one host and verify before proceeding. Update Board only while its callers
   are coordinated/quiescent; retain the same database and persistent tunnels.

```sh
# On the destination host, after infrastructure configuration:
# Detached systemd rollout survives its calling tool, including self-upgrade.
python3 -B "$release/native_release.py" launch --release "$release" \
  --host-id agent-lab-2 --components workspace,session-workspace,native,fleet
# Or stage + rolling activation over SSH:
python3 tooling/deploy_native.py "$release" --hosts borg-agent-lab-2 --activate
# Board/recovery are explicitly selected on agent-lab only:
python3 -B "$release/native_release.py" launch --release "$release" \
  --host-id agent-lab --components board,workspace,session-workspace,native,fleet,recovery
```

The installer uses SQLite's online backup API (including committed WAL data),
checks backup integrity, preserves old unit files, installs enabled services with
unlimited systemd restart, and requires actual JSON Board and MCP discovery
readiness. A SPA fallback returning HTTP 200 is not Board readiness. Startup
probes are bounded so systemd can retry failed startup; inference has no such
finite retry budget. Dependencies are ordered with Board/tunnel and workspace
before native, and native before Fleet/recovery.

Every activation writes `state/deployments/<timestamp>.json` with before/after
session receipt hashes, backup hashes and service state. Binary claims are
corroborated by SHA-256 of `/proc/<pid>/exe`, not a version string alone. A failed
rollout exits nonzero and retains evidence. It does not roll back a database or
blindly replay a session. Diagnose and repair with an owned task.

## Required acceptance evidence

Enabled services and health alone are insufficient. Record real Fleet host and
dispatch rows on all four labs; two same-identity sessions; parent lifecycle-MCP
self-delegation; filesystem sentinel isolation; exact preserved tool receipts
across restart; owned automatic failure diagnosis with a regression test; and a
native-authored tested commit deployed and published to GitHub. PM's independent
acceptance audit consumes these receipts. Missing evidence stays visibly unproven.

```sh
BORG_NATIVE_TEST_BIN="$PWD/.runtime/build/native/debug/borg-native" \
  python3 -m unittest discover -s tests -p 'test_native_d*.py' -v
python3 tooling/native_release.py inventory
python3 tooling/native_ready.py --board-api http://127.0.0.1:8079/api \
  --native-url http://127.0.0.1:8787 --workspace-url http://127.0.0.1:8766/mcp
```
