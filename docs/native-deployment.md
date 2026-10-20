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
| `borg-native` | all four labs; retained builder runtime | Existing `state/native.sqlite`, loopback 8787 |
| `borg-workspace` | all four labs | Retained original-session checkout MCP, loopback 8766 |
| `borg-session-workspace` | all four labs | Session-isolated MCP under `workspaces/{session_id}`, loopback 8791 |
| `borg-fleet` | all four labs | Board-authoritative capacity, dispatch, workspace and runtime reconciliation |
| `borg-recovery` | `agent-lab` only | Independent durable incident/outbox controller |

Release verification covers current native service state, selected images,
dependency readiness and durable session receipts. Credentials stay in existing
mode-0600 environment files and are neither packaged nor printed. Responses API
credentials and Codex login remain separate.

### Verified selected deployment (October 20, 2026)

One persistent Board and four enabled Fleet workers are live. Host configs use
Board-only identity configuration; model, tools, roles and retry policy are
metadata. Native session IDs, bindings, databases and completed receipts were
retained. The builder remains an observed control-plane host, capacity zero and
disabled for placement; one explicitly assigned Fleet observer reports its real
session progress without launching foreign work.

The final selected updates intentionally did not restart unchanged Native or
worker workspace processes. Their source trees are identical in the deployed
Native baseline and the final integrated source.

| Component | Verified deployed release |
| --- | --- |
| Shared Board and all four Fleet workers | `049de0c832471fee9efd1aebe3909d0762fdc579` |
| Native engines and four-lab workspace services | `175d295a2fc167d77948fd7c020e794480753468` |
| Independent recovery controller | `5ad61bf7a31f9473d89f170722e836714f7532ea` |
| Selected-update helper source correction | `2eaee3d8948fd3b4a3bd4daf282fb48dc0a8da4c` and its final documentation/audit descendants |

Task 28 completed the single linked repaired-lab1 execution in the existing
project/identity: real hostname/cwd/Git checkout, marker write/readback and a
normal cross-workspace denial. It supplements task 21's real delegation and
other-host receipts; it does not retroactively turn retained failures 7/10/11
into successes. The lineage-aware monitor automatically created owned task 27,
and its incident/task/dispatch plus frozen outbox/receipt hashes survived the
planned monitor restart on the same SQLite inode.

Project 1 / task 6 owns exact release, process, database, receipt and final Git
publication records. The [acceptance audit](native-acceptance.md) verifies the
explicit component revision map; a uniform revision check is not appropriate
for this selected deployment. No service is relabeled to imply an unused build.

## Build and stage

From a clean committed checkout:

```sh
# Locked dependency builds; independent component caches, without debug-symbol bloat.
# BORG_BUILD_PROFILE=release is also supported; default dev builds are debug=0.
tooling/build-native-release
# For a native-only hotfix, skip all Fleet/Board/frontend builds:
# BORG_BUILD_COMPONENTS=native tooling/build-native-release
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
configuration at inference boundaries. New identities inherit role metadata;
no per-identity host JSON or environment edits are needed. See
[metadata setup](native-metadata-deployment.md) and the
[profile contract](native-board-contract.md).

The selected model remains `gpt-6-astra`, max reasoning, 128000 output, full
shared 1050000-token context, no smaller input cap and no default turn cap.
The deployed retry profile is `max_retries: null`, `retry_initial_ms: 5000`,
`retry_max_ms: 60000`, `retry_jitter_percent: 20`. Transient inference and retryable
read-only profile/discovery failures retry indefinitely with durable visible
status. Local exponential delay and jitter are capped at 60 seconds; server
`Retry-After` floors may exceed that cap. Legacy numeric retry limits never
terminate recoverable work. Operator stop remains effective; uncertain tool
writes require original-result reconciliation, not automatic replay.

Fleet starts with:

```sh
fleet --config /home/dev-user/.local/share/borg/config/native-fleet.toml native-worker
```

Its top-level `board_api` is `http://127.0.0.1:8079/api` and `host` identifies the
lab. `[native]` supplies the advertised `runtime_url` (ending `/api`),
`workspace_root`, `workspace_mcp_url = "http://127.0.0.1:8791/mcp/{session_id}"`,
initial `capacity`, `poll_seconds`, `repo_url`, `repo_ref` and `git_ssh_config`.
Shared workers omit `project_id` so new Board projects need no host edits; an
explicit value intentionally scopes a test worker. Host capacity and enablement
are then authoritative Board metadata, not overwritten by each heartbeat.

Board placement reserves a session UUID on a fresh eligible host using the
lowest occupied/capacity ratio. Each session gets an independent checkout and
`native/session-<UUID>` branch. Identity/task placement and workspace metadata
can constrain selection; observational identity `host` or `workspace_cwd` is not
a session binding. Runtime origins and Fleet REST URLs must identify the same
host from every lifecycle caller; see [four-host routes](native-fleet.md).
Existing adopted sessions keep their original IDs, endpoints and workspaces.

## Guarded activation

The task's integrator owns publication and activation. Worker sessions deliver
tested commits from their own branches; they do not restart services or edit
another session's checkout to integrate a change.

1. Select only changed components. For a Board/Fleet-only update use the
   per-host installer: `board,fleet` on the controller and `fleet` on other labs,
   omitting the builder. It retains online backups, actual image/readiness checks
   and local Fleet admission pause, but does not drain/resume Native sessions or
   restart Native/workspace services. A recovery-only update is independent of
   Fleet and watched-service readiness.
2. **Never cancel an in-flight tool to force a rollout.** When Native or a
   workspace service is actually selected, the installer sets the durable stop
   flag without cancellation and waits for original receipt boundaries. It does
   not infer dispatch exclusion from progress text. A slow live model call is
   not proof of death.
3. Reconcile truly uncertain outcomes using original receipts and external
   effects, not by repeating the write. A durable drain journal owns each
   deployment pause. Timeout cleanup remains resume-pending until the runtime
   actually clears its stop flag. Detached systemd jobs use `--no-block`: the
   initiating tool returns its receipt before anyone polls its own drain.
4. Retain state paths, credentials and the shared database. Activate and verify
   selected images, actual readiness and preserved receipts before resuming
   work. A failed or unverified image must not receive new admission.

```sh
# On one destination host, for an already staged per-host release:
release="/home/dev-user/.local/share/borg/releases/$revision"
python3 -B "$release/native_release.py" launch --release "$release" \
  --host-id agent-lab-2 --components workspace,session-workspace,native,fleet
# Or stage + rolling per-host activation over SSH:
python3 tooling/deploy_native.py "$release" --hosts borg-agent-lab-2 --activate
```

For ordinary selected Board/Fleet updates, use the per-host path above with
`--components board,fleet` on `agent-lab` and `--components fleet` on workers.
Build from the corrected helper source; never edit a frozen release manifest.
The detached whole-cluster coordinator is for an intentional Native/profile
transition, not a Board-only patch: it drains all five callers and its target
list does not install Fleet. Use it only when that wider transition is wanted.

```sh
# Selected controller update; Native/workspace images and drivers are not selected:
python3 -B "$release/native_release.py" launch --release "$release" \
  --host-id agent-lab --components board,fleet
# Intentional whole-cluster Native transition, when required:
python3 tooling/native_rollout.py launch --release "$release" --components board,native
```

Routine updates do not need `--board-profiles`: the one-time profile/binding
migration is already complete. Do not change identity workspace metadata to
reroute an existing session or its committed tools. Board and recovery component
installs are restricted to `agent-lab`.

The installer uses SQLite's online backup API (including committed WAL data),
checks backup integrity, preserves old unit files, and installs enabled services
with unlimited systemd restart. Native/Fleet startup requires real JSON Board
and native/workspace readiness, not a SPA fallback returning HTTP 200. Startup
probes are bounded so systemd can retry; inference has no finite retry budget.
Dependencies order Board/tunnel and workspace before native, and native before
Fleet. Recovery has network-only ordering and no watched-service readiness
precondition, so a Board/native outage cannot prevent its own retry loop from
starting.

Every activation writes `state/deployments/<timestamp>.json` with backup hashes,
selected image and service state. Native/workspace transitions additionally
record before/after session receipt hashes; a selected Board/Fleet update does
not pretend to perform a Native drain or process-restart test. Binary claims are
corroborated by SHA-256 of `/proc/<pid>/exe`, not a version string alone. The
cluster coordinator also writes `state/cluster-rollouts/<transaction>.json`.
A failed install retains recovery evidence and does not replace a database or
blindly replay a session. If shared Board activation is uncertain, the detached
coordinator reconciles the original/selected image, configuration, readiness and
unchanged database identity; if unavailable, its recovery continuation remains
alive instead of leaving the paused integrator as the only repair path.

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
python3 -m unittest discover -s tests -p test_native_rollout.py -v
python3 tooling/native_release.py inventory
python3 tooling/native_ready.py --board-api http://127.0.0.1:8079/api \
  --native-url http://127.0.0.1:8787 --workspace-url http://127.0.0.1:8766/mcp
```
