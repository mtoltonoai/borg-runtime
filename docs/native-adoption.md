# Bootstrap adoption before Fleet admission

`tooling/native_adopt.py` implements the existing Fleet bootstrap boundary. Run
it **after the integrator has safely bound original sessions** to their host,
workspace, advertised runtime origin and generation, and **before Fleet is
enabled**. It never provisions, launches, stops, resumes, binds, resolves a tool,
changes a native database or activates a service. Native/profile switching and
cluster drain/resume remain in the integrator-owned tooling.

## Manifest and invocation

A manifest names the already-running runtimes and their host registry seeds.
An optional `session_ids` list selects the bootstrap cohort; omit it to examine
that runtime's full inventory. The helper reads all original binding fields from
the bound session, not from mutable identity metadata or guessed workspace paths.

```json
{
  "hosts": [
    {
      "host_id": "agent-lab",
      "runtime_url": "http://127.0.0.1:17871",
      "workspace_root": "/home/dev-user/.local/share/borg/workspaces",
      "capacity": 4
    },
    {
      "host_id": "agent-lab-2",
      "runtime_url": "http://127.0.0.1:17872",
      "workspace_root": "/home/dev-user/.local/share/borg/workspaces",
      "capacity": 4
    },
    {
      "host_id": "agent-lab-3",
      "runtime_url": "http://127.0.0.1:17873",
      "workspace_root": "/home/dev-user/.local/share/borg/workspaces",
      "capacity": 4
    },
    {
      "host_id": "agent-lab-4",
      "runtime_url": "http://127.0.0.1:17874",
      "workspace_root": "/home/dev-user/.local/share/borg/workspaces",
      "capacity": 4
    },
    {
      "host_id": "borg-builder",
      "runtime_url": "http://127.0.0.1:17870",
      "probe_url": "http://127.0.0.1:8787",
      "workspace_root": "/home/dev-user/.local/share/borg/workspaces",
      "capacity": 0,
      "enabled": false
    }
  ]
}
```

The builder example is for invoking the helper **on the builder**. From a lab,
omit its `probe_url` to read it through the advertised `17870` route. Both origins
and `/api` bases are accepted and normalized. `probe_url` only selects the read
transport; it never rewrites the session's recorded advertised `runtime_url`.
The latter must match the host declaration, otherwise preflight reports a real
profile/binding mismatch. Use the previously installed routes; do not substitute
builder-local `8787` into a globally advertised dispatch row.

```sh
# Read-only preflight; performs no Board writes and never claims enablement safety.
python3 tooling/native_adopt.py \
  --board-api http://127.0.0.1:8079/api \
  --manifest /absolute/bootstrap-adoption.json \
  --output /absolute/adoption-plan.json

# Integrator's activation coordinator calls this after binding, before Fleet.
python3 tooling/native_adopt.py \
  --board-api http://127.0.0.1:8079/api \
  --manifest /absolute/bootstrap-adoption.json \
  --apply --output /absolute/adoption-receipts.json
```

For authenticated deployments, `--board-token-env NAME` and per-host
`bearer_token_env` refer to existing credential environment variables. No token
value belongs in the manifest, logs or source. The helper never reads SSH keys.

## Semantics

- Preflights the **whole cohort before writes**. Duplicate live task claims,
  wrong owner/host, missing binding, unsafe paths, conflicting existing receipts
  and missing selected sessions are concrete failures, not grounds for creating
  a replacement runtime.
- Creates/refreshes Board host observations, preserving existing capacity and
  operator drain policy. A new bootstrap-only builder is capacity zero and
  disabled, not a fifth placement worker. Runtime readiness is reported honestly
  while the coordinator is draining. No invented deployed revision is stamped.
- Adopts with `task:{id}:attempt:{task.metadata.native_attempt or 1}` and the
  original session id, identity, task, host, workspace, generation and endpoint.
  It omits kickoff/input overrides. Registry status is the actual session state.
- Records `stopped` legacy drain posture, `failed` repair state and
  `needs_resolution` uncertainty **as-is**. It never resumes or resolves them,
  and does not pretend a stopped driver is running. Their durable canonical
  receipt prevents the normal worker from creating a second task execution.
- Skips done/cancelled/iceboxed/archived tasks and completed/cancelled sessions.
  A completed session with a still-actionable task and no prior canonical receipt
  is reported in `uncovered_terminal_tasks`; it is **not** silently declared
  adopted or safe to replay. The rollout/recovery owner must disposition that
  concrete work (or keep its explicit `native_dispatch:false` hold).
- Reads Board before posting, verifies every immutable binding field, and reads
  back successful posts. After a lost reply it looks up the canonical request key
  first. A committed receipt is returned without another post. If absent, only
  the same idempotent **Board adoption** may retry; no runtime operation retries.
- Re-running after interruption is safe. A changed binding never overwrites the
  original record. Terminal status drift does not change immutable identity.
- JSON reports `plan_valid`, `applied`, `safe_to_enable_fleet`, adopted receipts,
  skipped reasons and uncovered tasks. Dry-run always has
  `safe_to_enable_fleet:false`. Exit zero means a valid plan/result; activation
  callers must require `applied:true` and `safe_to_enable_fleet:true` after
  `--apply`, in addition to their existing image/readiness/drain checks.

The helper does not synthesize a background observer for the builder. Its
original native reporting/recovery path remains responsible for later progress;
it never lies about the host merely to attach the session to a lab worker.

## Tests

```sh
cd components/task-board && cargo build --locked --bin task-board
cd ../..
python3 tests/test_native_adopt.py
```

Eleven tests include an actual isolated Board process/SQLite registry: five-host
adoption including builder, exact repeats, injected lost replies before and after
the real commit, changed binding rejection, duplicate task preflight, completed
work skipping, zero-write dry run, and capacity/drain preservation. Native
fixtures reject every non-GET operation and original workspace paths remain
uncreated, proving the helper does not provision or mutate original sessions.
