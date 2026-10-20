# Native deployment acceptance

Task Board project 1 is the authority for acceptance, owners and repair work.
This file documents the executable evidence collector, not a prerequisite or
approval gate. Deployments must remain executable while agents keep working.

## Read-only operational audit

`tooling/native_acceptance.py` collects real JSON from the shared Board and each
native service. It never stops, resumes, dispatches, resolves or replays work.
A failed check is evidence to diagnose, **not authority to kill a slow model or
replay an unknown tool write**.

From agent-lab (the supplied port forwards are local to this host):

```sh
python3 tooling/native_acceptance.py \
  --board http://127.0.0.1:8079 \
  --runtime agent-lab=http://127.0.0.1:8787 \
  --runtime agent-lab-2=http://127.0.0.1:17872 \
  --runtime agent-lab-3=http://127.0.0.1:17873 \
  --runtime agent-lab-4=http://127.0.0.1:17874 \
  --runtime borg-builder=http://127.0.0.1:17870 \
  --project 1 --expected-revision "$(git rev-parse HEAD)" \
  --snapshot-out .runtime/acceptance/snapshot.json \
  --report-out .runtime/acceptance/report.json
```

The extra builder endpoint observes the integrator's bootstrap session; it never
substitutes for any of the four required lab workers. On another host, supply
that host's actual port forwards. Optional `--board-token-env NAME` and
`--native-token-env NAME` read credentials from existing environment variables;
credentials are not command-line values. Base URLs cannot carry authentication,
queries or fragments; HTTP redirects are refused.

Exit codes: **0** operational checks passed, **1** observed requirements failed,
**2** invalid arguments/evidence. A complete saved JSON report is emitted even
when endpoints are unavailable. The command is deliberately strict against the
bootstrap: absent Fleet endpoints, missing process/host bindings and finite
retry policies are failures, not silently skipped checks. A full expected Git
revision is required to pass. Evidence is time-bounded; replaying an old healthy
snapshot with `--snapshot-in` does not establish current readiness.

The collector hashes completed tool results and stores only allowlisted
operational fields. It does not retain raw tool arguments/results, instructions,
transcripts, Board descriptions, arbitrary metadata or response error bodies.
Keep snapshots in `.runtime/` (Git-ignored); do not commit live databases/logs.

## Contract and interpretation

- Board readiness is **`/api/health`** (`ok`, `db`, `commit`). `/health` may be the
  UI's HTML fallback and must never pass a readiness check.
- Board `/api/fleet/hosts` and `/api/fleet/dispatches` return arrays. Dispatches default to 100 rows; the
  collector requests 1000-row pages until exhaustion and rejects repeated IDs. Hosts expose
  configured capacity, runtime readiness, freshness/TTL, epoch-second heartbeat
  and deployed revision. Dispatches carry request/task/identity/host/session IDs,
  generation, workspace path and status. Reservations are counted until a
  confirmed terminal status; `needs_resolution` still occupies capacity.
- Native `/health` exposes `status`, `host_id`, `revision`, and process-unique
  `instance_id`. `/api/agents` exposes the effective model/retry profile;
  `/api/sessions` exposes independent `placement`, `driver_running` and phase.
  `/api/sessions/{id}/tools` is read for result custody receipts.
- Identity, task, dispatch and session IDs remain separate. A duplicate request
  cannot cause a second session. Identical active session IDs on multiple hosts
  fail. Configured host placement is separate from observational agent `host`.
- The selected profile remains `gpt-6-astra` / `max`, 128000 output, full 1050000
  shared context, and `max_turns: null`. Chosen retry policy is
  `max_retries: null`, `retry_initial_ms: 5000`, `retry_max_ms: 60000`,
  `retry_jitter_percent: 20`. Transient inference and retryable read-only
  discovery never fail solely on budget exhaustion. Honor safe Retry-After/rate
  reset hints, keep retry status visible, preserve explicit operator stop, and
  reconcile unknown writes. Bootstrap `4294967295` is only temporary mitigation.
- A task is covered by a same-owner live session bound to the task or its
  ancestor, a fresh concrete dispatch, an explicit owned blocked/paused state,
  or `metadata.recovery_task_id` pointing to a repair with a live path. Merely
  marking an agent online or a task monitor-exempt is not coverage. Recovery
  cycles and finished repair tasks do not conceal orphaned work.
- Slow inference with an actual live driver remains live regardless of the last
  progress timestamp. A stale host or absent driver is a separate observation.

## Real restart receipt preservation

Collect `before.json`, let the integrator perform the automated cooperative
boundary drain/roll/resume, then rerun the live command with:

```sh
# Add these arguments to the live collection command above:
--before .runtime/acceptance/before.json \
--snapshot-out .runtime/acceptance/after.json
```

The comparison requires a changed process instance on every required host,
preserved session identity/task bindings, and the same unique operation/call IDs
and hashes for every previously completed tool. No completed receipts or an
incomplete observation fails closed. This proves durable receipt preservation;
it does **not** by itself prove that an external side effect ran exactly once.
Unknown operations need explicit receipt reconciliation, never blind retry.

## Acceptance evidence still required

An operational audit is not the entire project acceptance demonstration. Record
these concrete receipts on the responsible Board task and task 1:

| Demonstration | Owner | Required evidence |
| --- | --- | --- |
| Board-defined identity/profile/tools | native-core | New identity and role/grant update work without host JSON edits; effective profile observed |
| Native lifecycle delegation | tools-engineer | Parent's successful lifecycle MCP receipt, child dispatch/session IDs, same identity, independent state |
| Four-host balancing | fleet-engineer | Controlled useful dispatch batch, fresh capacity records, all four placements, concurrent duplicate request test |
| Filesystem isolation | fleet-engineer + tools-engineer | Distinct real roots; unique sentinels and failed cross-read; not merely different path strings |
| Persistence and revision | integrator | Enabled/active services, actual binary revisions/checksums, restart instance changes, unchanged DB/session/result receipts |
| Durable repair | recovery-engineer | Injected failure, deduplicated owned incident, diagnosis, source repair, regression test and deployed revision |
| Native-authored delivery | integrator | Tested worker commit, integrated ancestry, live revision and GitHub publication |
| No-orphan and UI accuracy | pm | Operational report, every actionable owner/live or visible recovery path, UI corroboration |

Read-only audit failures do not block engineers from making fixes or integrator
from rolling a repair. Do not waive missing evidence or declare the project done
because unit tests, `systemctl is-active`, or health endpoints pass.

## Tests

```sh
python3 -m unittest discover -s tests -p test_native_acceptance.py -v
python3 -m unittest discover -s tests -v
```

Tests exercise actual loopback HTTP (including misleading HTML 200), secret
redaction, missing/stale hosts, capacity/duplicate placement, profile regression,
indefinite retry visibility, owned repair coverage, slow live inference, and
lost/changed/duplicate receipts across a witnessed process restart.


## Fresh identity/project live cohort

`tooling/prepare_native_acceptance.py` prepares stable run IDs without remote
writes. Once the integrator has activated the full release, start the cohort:

```sh
python3 tooling/prepare_native_acceptance.py
python3 tooling/prepare_native_acceptance.py --start --revision FULL_INTEGRATED_SHA
```

It requires the actual four ready native hosts and shared Board at that revision,
fresh Fleet heartbeats and four free slots. It registers a fresh Board-only
`native-engineer` identity, verifies its full/unlimited inherited profile on all
four **unchanged process instances**, then creates a new project and one owned
actionable parent task. Fleet, not this script, schedules the parent. The parent
model uses its own lifecycle MCP tool to create three same-identity children;
all write/read the same relative marker in their isolated workspaces. Children
attempt a forbidden parent-path read and report with runtime-bound Board identity.
A declared 90-second parent tool barrier allows an actual overlap witness without
repeated model polling. No model/context/output limit is lowered.

The state file is `.runtime/acceptance/live-run.json`. Repeating `--start` reuses
the same named identity/project and task external reference, even after a lost
write reply; it does not manufacture child results or replay sessions. After
starting, pass the recorded project/task/identity/run IDs to the tools and Fleet
owners for real child receipts, overlap, host distribution and sentinel checks.
The returned scope explicitly says the live demonstration is not yet complete.
Also exercise PM cross-identity create/list/steer on a disposable session through
its real bound lifecycle MCP endpoint; do not stop/cancel a live unknown tool.


A verified live packaging issue in release `175d295` left the Board's
compile-time commit label `unknown` although its actual executable checksum was
verified by deployment. The cohort preparator permits functional probes in this
specific unreported-label case and returns `board_revision_verified: false`;
it still rejects a different known revision. This does not waive release
provenance: the operational audit's `board_revision` check continues to fail
until the integrator rebuilds/installs the correctly stamped Board. A repeated
cohort start reports `fresh_registration: false`, rather than pretending an
already existing identity was newly created again.
